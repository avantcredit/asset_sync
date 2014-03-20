module AssetSync
  class Storage
    REGEXP_FINGERPRINTED_FILES = /^(.*)\/([^-]+)-[^\.]+\.([^\.]+)$/

    class BucketNotFound < StandardError;
    end

    attr_accessor :config

    def initialize(cfg)
      @config = cfg
    end

    def connection
      @connection ||= Fog::Storage.new(self.config.fog_options)
    end

    def bucket
      # fixes: https://github.com/rumblelabs/asset_sync/issues/18
      @bucket ||= connection.directories.get(self.config.fog_directory, :prefix => self.config.assets_prefix)
    end

    def log(msg)
      AssetSync.log(msg)
    end

    def keep_existing_remote_files?
      self.config.existing_remote_files?
    end

    def path
      self.config.public_path
    end

    def ignored_files
      files = []
      Array(self.config.ignored_files).each do |ignore|
        case ignore
        when Regexp
          files += self.local_files.select do |file|
            file =~ ignore
          end
        when String
          files += self.local_files.select do |file|
            file.split('/').last == ignore
          end
        else
          log "Error: please define ignored_files as string or regular expression. #{ignore} (#{ignore.class}) ignored."
        end
      end
      files.uniq
    end

    def local_files
      @local_files ||= get_local_files
    end

    def always_upload_files
      self.config.always_upload.map { |f| File.join(self.config.assets_prefix, f) }
    end

    def files_with_custom_headers
      self.config.custom_headers.inject({}) { |h,(k, v)| h[File.join(self.config.assets_prefix, k)] = v; h; }
    end

    def files_to_invalidate
      self.config.invalidate.map { |filename| File.join("/", self.config.assets_prefix, filename) }
    end

    def get_local_files
      if self.config.manifest && File.exists?(self.config.manifest_path)
        return local_file_list_from_manifest
      else
        log "Warning: Manifest could not be found"
        log "Using: Directory Search of #{path}/#{self.config.assets_prefix}"
        Dir.chdir(path) do
          Dir["#{self.config.assets_prefix}/**/**"]
        end
      end
    end

    def local_files_from_rails_4
      log "Using: Rails 4.0 manifest access"
      manifest = Sprockets::Manifest.new(ActionView::Base.assets_manifest.environment, ActionView::Base.assets_manifest.dir)
      return manifest.assets.values.map { |f| File.join(self.config.assets_prefix, f) }
    end

    def local_files_from_rails_3
      yml = YAML.load(IO.read(self.config.manifest_path))

       file_set = yml.map do |original, compiled|
        # Upload font originals and compiled
        if original =~ /^.+(eot|svg|ttf|woff)$/
          [original, compiled]
        else
          compiled
        end
      end

      file_set.flatten.map { |f| File.join(self.config.assets_prefix, f) }.uniq
    end

    def local_file_list_from_manifest
      log "Using: Manifest #{self.config.manifest_path}"
      return local_files_from_rails_4 if ActionView::Base.respond_to?(:assets_manifest)

      local_files_from_rails_3
    end

    def get_remote_files
      raise BucketNotFound.new("#{self.config.fog_provider} Bucket: #{self.config.fog_directory} not found.") unless bucket
      # fixes: https://github.com/rumblelabs/asset_sync/issues/16
      #        (work-around for https://github.com/fog/fog/issues/596)
      files = []
      bucket.files.each { |f| files << f.key }
      return files
    end

    def delete_file(f, remote_files_to_delete)
      if remote_files_to_delete.include?(f.key)
        log "Deleting: #{f.key}"
        f.destroy
      end
    end

    def delete_extra_remote_files
      log "Fetching files to flag for delete"
      remote_files = get_remote_files
      # fixes: https://github.com/rumblelabs/asset_sync/issues/19
      from_remote_files_to_delete = remote_files - local_files - ignored_files - always_upload_files

      log "Flagging #{from_remote_files_to_delete.size} file(s) for deletion"
      # Delete unneeded remote files
      bucket.files.each do |f|
        delete_file(f, from_remote_files_to_delete)
      end
    end

    def upload_file(f)
      # TODO output files in debug logs as asset filename only.
      one_year     = 31557600
      # Check if the incoming file is the uncompressed or compressed version
      is_file_gz   = File.extname(f) == ".gz"

      file_name    = is_file_gz ? f.sub(/\.gz?/,'') : f
      gz_file_name = is_file_gz ? f : "#{f}.gz"

      file_path              = "#{path}/#{file_name}"
      gz_file_path           = "#{path}/#{gz_file_name}"
      ext                    = File.extname(file_name)[1..-1]
      mime                   = MultiMime.lookup(ext)

      file_payload = {
        :key          => file_name,
        :content_type => mime,
        :public       => true
      }

      if /-[0-9a-fA-F]{32}$/.match(File.basename(f,File.extname(f)))
        file_payload.merge!({
          :cache_control => "public, max-age=#{one_year}",
          :expires => CGI.rfc1123_date(Time.now + one_year)
        })
      end

      # overwrite headers if applicable, you probably shouldn't specific key/body, but cache-control headers etc.
      # TODO we may want this group to go against file_name
      if files_with_custom_headers.has_key? f
        file_payload.merge! files_with_custom_headers[f]
        log "Overwriting #{f} with custom headers #{files_with_custom_headers[f].to_s}"
      elsif key = self.config.custom_headers.keys.detect {|k| f.match(Regexp.new(k))}
        headers = {}
        self.config.custom_headers[key].each do |key, value|
          headers[key.to_sym] = value
        end
        file_payload.merge! headers
        log "Overwriting matching file #{f} with custom headers #{headers.to_s}"
      end

      if ignore = (config.gzip? && is_file_gz)
        # Don't bother uploading gzipped assets if we are in gzip_compression mode
        # as we will overwrite file.css with file.css.gz if it exists.
        log "Ignoring #{f}"
      elsif config.gzip? && File.exists?(gz_file_path)
        original_size = File.size(file_path)
        gzipped_size  = File.size(gz_file_path)

        percentage = percentage_change(original_size, gzipped_size)
        if gzipped_size < original_size
          file_payload.merge!({
            :body             => File.open(gz_file_path),
            :content_encoding => 'gzip'
          })
          log "Uploading #{gz_file_name} in place of #{file_name}, saving #{percentage}%"
        else
          log "Uploading #{file_name} instead of #{gz_file_name} because compression increases the file size by #{-1 * percentage}%"
        end
      else
        if !config.gzip? && is_file_gz
          # set content encoding for gzipped files this allows cloudfront to properly handle requests with Accept-Encoding
          # http://docs.amazonwebservices.com/AmazonCloudFront/latest/DeveloperGuide/ServingCompressedFiles.html
          file_payload.merge!({
            :key              => gz_file_name,
            :body             => File.open(gz_file_path),
            :content_encoding => 'gzip'
          })
        end
        log "Uploading #{file_name}"
      end

      if config.aws? && config.aws_rrs?
        file_payload.merge!({
          :storage_class => 'REDUCED_REDUNDANCY'
        })
      end

      file_payload.merge!(:body => File.open(file_path)) unless file_payload.has_key?(:body)

      log file_payload.inspect

      bucket.files.create( file_payload ) unless ignore
    end

    def upload_files
      # get a fresh list of remote files
      remote_files = ignore_existing_remote_files? ? [] : get_remote_files
      # fixes: https://github.com/rumblelabs/asset_sync/issues/19
      local_files_to_upload = local_files - ignored_files - remote_files + always_upload_files
      local_files_to_upload = (local_files_to_upload + get_non_fingerprinted(local_files_to_upload)).uniq

      log "AssetSync: Uploading files..." unless local_files_to_upload.empty?

      # Upload new files
      local_files_to_upload.each do |f|
        next unless File.file? "#{path}/#{f}" # Only files.
        upload_file f
      end

      log "AssetSync: Uploading finished" unless local_files_to_upload.empty?

      if self.config.cdn_distribution_id && files_to_invalidate.any?
        log "Invalidating Files"
        cdn ||= Fog::CDN.new(self.config.fog_options.except(:region))
        data = cdn.post_invalidation(self.config.cdn_distribution_id, files_to_invalidate)
        log "Invalidation id: #{data.body["Id"]}"
      end
    end

    def sync
      # fixes: https://github.com/rumblelabs/asset_sync/issues/19
      log "AssetSync: Syncing..."
      upload_files
      delete_extra_remote_files unless keep_existing_remote_files?
      log "AssetSync: Done."
    end

    private

    def ignore_existing_remote_files?
      self.config.existing_remote_files == 'ignore'
    end

    def get_non_fingerprinted(files)
      files.map do |file|
        match_data = file.match(REGEXP_FINGERPRINTED_FILES)
        match_data && "#{match_data[1]}/#{match_data[2]}.#{match_data[3]}"
      end.compact
    end

    def percentage_change(original_size, new_size)
      "%.2f" % (((original_size.to_f - new_size.to_f) / original_size.to_f) * 100)
    end

  end
end
