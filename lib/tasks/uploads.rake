require "digest/sha1"

################################################################################
#                                    gather                                    #
################################################################################

task "uploads:gather" => :environment do
  require "db_helper"

  ENV["RAILS_DB"] ? gather_uploads : gather_uploads_for_all_sites
end

def gather_uploads_for_all_sites
  RailsMultisite::ConnectionManagement.each_connection { gather_uploads }
end

def file_exists?(path)
  File.exists?(path) && File.size(path) > 0
rescue
  false
end

def gather_uploads
  public_directory = "#{Rails.root}/public"
  current_db = RailsMultisite::ConnectionManagement.current_db

  puts "", "Gathering uploads for '#{current_db}'...", ""

  Upload.where("url ~ '^\/uploads\/'")
        .where("url !~ '^\/uploads\/#{current_db}'")
        .find_each do |upload|
    begin
      old_db = upload.url[/^\/uploads\/([^\/]+)\//, 1]
      from = upload.url.dup
      to = upload.url.sub("/uploads/#{old_db}/", "/uploads/#{current_db}/")
      source = "#{public_directory}#{from}"
      destination = "#{public_directory}#{to}"

      # create destination directory & copy file unless it already exists
      unless file_exists?(destination)
        `mkdir -p '#{File.dirname(destination)}'`
        `cp --link '#{source}' '#{destination}'`
      end

      # ensure file has been succesfuly copied over
      raise unless file_exists?(destination)

      # remap links in db
      DbHelper.remap(from, to)
    rescue
      putc "!"
    else
      putc "."
    end
  end

  puts "", "Done!"

end

################################################################################
#                                backfill_shas                                 #
################################################################################

task "uploads:backfill_shas" => :environment do
  RailsMultisite::ConnectionManagement.each_connection do |db|
    puts "Backfilling #{db}..."
    Upload.where(sha1: nil).find_each do |u|
      begin
        path = Discourse.store.path_for(u)
        u.sha1 = Digest::SHA1.file(path).hexdigest
        u.save!
        putc "."
      rescue Errno::ENOENT
        putc "X"
      end
    end
  end
  puts "", "Done"
end

################################################################################
#                               migrate_from_s3                                #
################################################################################

task "uploads:migrate_from_s3" => :environment do
  require "db_helper"

  ENV["RAILS_DB"] ? migrate_from_s3 : migrate_all_from_s3
end

def guess_filename(url, raw)
  begin
    uri = URI.parse("http:#{url}")
    f = uri.open("rb", read_timeout: 5, redirect: true, allow_redirections: :all)
    filename = if f.meta && f.meta["content-disposition"]
      f.meta["content-disposition"][/filename="([^"]+)"/, 1].presence
    end
    filename ||= raw[/<a class="attachment" href="(?:https?:)?#{Regexp.escape(url)}">([^<]+)<\/a>/, 1].presence
    filename ||= File.basename(url)
    filename
  rescue
      nil
  ensure
    f.try(:close!) rescue nil
  end
end

def migrate_all_from_s3
  RailsMultisite::ConnectionManagement.each_connection { migrate_from_s3 }
end

def migrate_from_s3
  require "file_store/s3_store"

  # make sure S3 is disabled
  if SiteSetting.enable_s3_uploads
    puts "You must disable S3 uploads before running that task."
    return
  end

  # make sure S3 bucket is set
  if SiteSetting.s3_upload_bucket.blank?
    puts "The S3 upload bucket must be set before running that task."
    return
  end

  db = RailsMultisite::ConnectionManagement.current_db

  puts "Migrating uploads from S3 to local storage for '#{db}'..."

  s3_base_url = FileStore::S3Store.new.absolute_base_url
  max_file_size_kb = [SiteSetting.max_image_size_kb, SiteSetting.max_attachment_size_kb].max.kilobytes

  Post.unscoped.find_each do |post|
    if post.raw[s3_base_url]
      post.raw.scan(/(#{Regexp.escape(s3_base_url)}\/(\d+)(\h{40})\.\w+)/).each do |url, id, sha|
        begin
          puts "POST ID: #{post.id}"
          puts "UPLOAD ID: #{id}"
          puts "UPLOAD SHA: #{sha}"
          puts "UPLOAD URL: #{url}"
          if filename = guess_filename(url, post.raw)
            puts "FILENAME: #{filename}"
            file = FileHelper.download("http:#{url}", 20.megabytes, "from_s3", true)
            if upload = Upload.create_for(post.user_id || -1, file, filename, File.size(file))
              post.raw = post.raw.gsub(/(https?:)?#{Regexp.escape(url)}/, upload.url)
              post.save
              post.rebake!
              puts "OK :)"
            else
              puts "KO :("
            end
            puts post.full_url, ""
          else
            puts "NO FILENAME :("
          end
        rescue => e
          puts "EXCEPTION: #{e.message}"
        end
      end
    end
  end

  puts "Done!"
end

################################################################################
#                                migrate_to_s3                                 #
################################################################################

task "uploads:migrate_to_s3" => :environment do
  require "file_store/s3_store"
  require "file_store/local_store"
  require "db_helper"

  ENV["RAILS_DB"] ? migrate_to_s3 : migrate_to_s3_all_sites
end

def migrate_to_s3_all_sites
  RailsMultisite::ConnectionManagement.each_connection { migrate_to_s3 }
end

def migrate_to_s3
  # make sure s3 is enabled
  if !SiteSetting.enable_s3_uploads
    puts "You must enable s3 uploads before running that task"
    return
  end

  db = RailsMultisite::ConnectionManagement.current_db

  puts "Migrating uploads to S3 (#{SiteSetting.s3_upload_bucket}) for '#{db}'..."

  # will throw an exception if the bucket is missing
  s3 = FileStore::S3Store.new
  local = FileStore::LocalStore.new

  # Migrate all uploads
  Upload.where.not(sha1: nil)
        .where("url NOT LIKE '#{s3.absolute_base_url}%'")
        .find_each do |upload|
    # remove invalid uploads
    if upload.url.blank?
      upload.destroy!
      next
    end
    # store the old url
    from = upload.url
    # retrieve the path to the local file
    path = local.path_for(upload)
    # make sure the file exists locally
    if !path or !File.exists?(path)
      putc "X"
      next
    end

    begin
      file = File.open(path)
      content_type = `file --mime-type -b #{path}`.strip
      to = s3.store_upload(file, upload, content_type)
    rescue
      putc "X"
      next
    ensure
      file.try(:close!) rescue nil
    end

    # remap the URL
    DbHelper.remap(from, to)

    putc "."
  end
end

################################################################################
#                                  clean_up                                   #
################################################################################

task "uploads:clean_up" => :environment do

  RailsMultisite::ConnectionManagement.each_connection do |db|
    puts "Cleaning up uploads and thumbnails for '#{db}'..."

    if Discourse.store.external?
      puts "This task only works for internal storages."
      next
    end

    public_directory = "#{Rails.root}/public"

    ##
    ## DATABASE vs FILE SYSTEM
    ##

    # uploads & avatars
    Upload.find_each do |upload|
      path = "#{public_directory}#{upload.url}"
      if !File.exists?(path)
        upload.destroy rescue nil
        putc "#"
      else
        putc "."
      end
    end

    # optimized images
    OptimizedImage.find_each do |optimized_image|
      path = "#{public_directory}#{optimized_image.url}"
      if !File.exists?(path)
        optimized_image.destroy rescue nil
        putc "#"
      else
        putc "."
      end
    end

    ##
    ## FILE SYSTEM vs DATABASE
    ##

    uploads_directory = "#{public_directory}/uploads/#{db}"

    # avatars (no avatar should be stored in that old directory)
    FileUtils.rm_rf("#{uploads_directory}/avatars") rescue nil

    # uploads
    Dir.glob("#{uploads_directory}/*/*.*").each do |f|
      url = "/uploads/#{db}/" << f.split("/uploads/#{db}/")[1]
      if !Upload.where(url: url).exists?
        FileUtils.rm(f) rescue nil
        putc "#"
      else
        putc "."
      end
    end

    # optimized images
    Dir.glob("#{uploads_directory}/_optimized/*/*/*.*").each do |f|
      url = "/uploads/#{db}/_optimized/" << f.split("/uploads/#{db}/_optimized/")[1]
      if !OptimizedImage.where(url: url).exists?
        FileUtils.rm(f) rescue nil
        putc "#"
      else
        putc "."
      end
    end

    puts

  end

end

################################################################################
#                                   missing                                    #
################################################################################

# list all missing uploads and optimized images
task "uploads:missing" => :environment do

  public_directory = "#{Rails.root}/public"

  RailsMultisite::ConnectionManagement.each_connection do |db|

    if Discourse.store.external?
      puts "This task only works for internal storages."
      next
    end


    Upload.find_each do |upload|

      # could be a remote image
      next unless upload.url =~ /^\/[^\/]/

      path = "#{public_directory}#{upload.url}"
      bad = true
      begin
        bad = false if File.size(path) != 0
      rescue
        # something is messed up
      end
      puts path if bad
    end

    OptimizedImage.find_each do |optimized_image|

      # remote?
      next unless optimized_image.url =~ /^\/[^\/]/

      path = "#{public_directory}#{optimized_image.url}"

      bad = true
      begin
        bad = false if File.size(path) != 0
      rescue
        # something is messed up
      end
      puts path if bad
    end

  end

end

################################################################################
#                        regenerate_missing_optimized                          #
################################################################################

# regenerate missing optimized images
task "uploads:regenerate_missing_optimized" => :environment do
  ENV["RAILS_DB"] ? regenerate_missing_optimized : regenerate_missing_optimized_all_sites
end

def regenerate_missing_optimized_all_sites
  RailsMultisite::ConnectionManagement.each_connection { regenerate_missing_optimized }
end

def regenerate_missing_optimized
  db = RailsMultisite::ConnectionManagement.current_db

  puts "Regenerating missing optimized images for '#{db}'..."

  if Discourse.store.external?
    puts "This task only works for internal storages."
    return
  end

  public_directory = "#{Rails.root}/public"
  missing_uploads = Set.new

  OptimizedImage.includes(:upload)
                .where("LENGTH(COALESCE(url, '')) > 0")
                .where("width > 0 AND height > 0")
                .find_each do |optimized_image|

    upload = optimized_image.upload

    next unless optimized_image.url =~ /^\/[^\/]/
    next unless upload.url =~ /^\/[^\/]/

    thumbnail = "#{public_directory}#{optimized_image.url}"
    original = "#{public_directory}#{upload.url}"

    if !File.exists?(thumbnail) || File.size(thumbnail) <= 0
      # make sure the original image exists locally
      if (!File.exists?(original) || File.size(original) <= 0) && upload.origin.present?
        # try to fix it by redownloading it
        begin
          downloaded = FileHelper.download(upload.origin, SiteSetting.max_image_size_kb.kilobytes, "discourse-missing", true) rescue nil
          if downloaded && downloaded.size > 0
            FileUtils.mkdir_p(File.dirname(original))
            File.open(original, "wb") { |f| f.write(downloaded.read) }
          end
        ensure
          downloaded.try(:close!) if downloaded.respond_to?(:close!)
        end
      end

      if File.exists?(original) && File.size(original) > 0
        FileUtils.mkdir_p(File.dirname(thumbnail))
        OptimizedImage.resize(original, thumbnail, optimized_image.width, optimized_image.height)
        putc "#"
      else
        missing_uploads << original
        putc "X"
      end
    else
      putc "."
    end
  end

  puts "", "Done"

  if missing_uploads.size > 0
    puts "Missing uploads:"
    missing_uploads.sort.each { |u| puts u }
  end
end

################################################################################
#                             migrate_to_new_scheme                            #
################################################################################

task "uploads:start_migration" => :environment do
  SiteSetting.migrate_to_new_scheme = true
  puts "Migration started!"
end

task "uploads:stop_migration" => :environment do
  SiteSetting.migrate_to_new_scheme = false
  puts "Migration stoped!"
end
