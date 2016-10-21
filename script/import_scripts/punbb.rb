require "pg"
require 'optparse'

require File.expand_path(File.dirname(__FILE__) + "/base.rb")

require 'pry'

# Call it like this:
#   RAILS_ENV=production bundle exec ruby script/import_scripts/punbb.rb
#   check usernames where imported correctly:
#   select u.username, cf.value from users as u, user_custom_fields as cf where u.id = cf.user_id and cf.name = 'import_username' and u.username != cf.value;
class ImportScripts::PunBB < ImportScripts::Base

  BATCH_SIZE = 500
  GROUPS_ASSOCE = [5, 8, 13, 15, 26]
  GROUPS_ANCIENS = [10]
  GROUPS_CA = [6, 15, 21]
  GROUPS_MODOS_TOPO = [11, 13, 15, 16, 26]
  GROUPS_PUB = [22, 26]
  GROUPS_PARTNERS = [27]

  VIRTUAL_GROUP_ASSOCE_ID = 1
  VIRTUAL_GROUP_ANCIENTS_ID = 2
  VIRTUAL_GROUP_CA_ID = 3
  VIRTUAL_GROUP_MODOS_TOPO_ID = 4
  VIRTUAL_GROUP_PUB_ID = 5
  VIRTUAL_GROUPS_PARTNERS_ID = 6

  def initialize
    step "initialize"

    super

    @client = PG::Connection.open(
      :host => ENV["V5_PGHOST"],
      :user => ENV["V5_PGUSER"],
      :password => ENV["V5_PGPASSWORD"],
      :dbname => ENV["V5_PGDATABASE"]
    )

    @options = {}
    OptionParser.new do |opts|
      opts.banner = "Usage: #{$0} [options]"

      opts.on("-t", "--topic=TOPIC_ID", "Filter source records by topics_id") do |topic|
        @options[:topic] = topic
      end
      opts.on("-t", "--firstpost=FIRST_POST_ID", "Start at this first post") do |firstpost|
        @options[:firstpost] = firstpost
      end
    end.parse!
  end

  def execute
    import_groups
    import_users
    import_categories
    import_posts
    suspend_users

    create_categories_permalinks
    create_posts_permalinks
    create_topics_permalinks
  end

  def import_groups
    step "creating groups"

    groups = [
    {id: VIRTUAL_GROUP_ASSOCE_ID, name: "Association"},
    {id: VIRTUAL_GROUP_ANCIENTS_ID, name: "Ancien_membre"},
    {id: VIRTUAL_GROUP_CA_ID, name: "CA"},
    {id: VIRTUAL_GROUP_MODOS_TOPO_ID, name: "Modo_Topoguide"},
    {id: VIRTUAL_GROUP_PUB_ID, name: "Pub"},
    {id: VIRTUAL_GROUPS_PARTNERS_ID, name: "Partenaires"}
    ]
    create_groups(groups) do |group|
      group
    end
  end

  def import_users
    step "creating users"

    sql = "
      SELECT count(*) count
      FROM app_users_private_data"
    sql += "
      WHERE app_users_private_data.id IN (
        SELECT poster_id FROM punbb_posts WHERE punbb_posts.topic_id = #{@options[:topic]}
      )" if @options[:topic]
    total_count = sql_query(sql).first['count']

    batches(BATCH_SIZE) do |offset|
      sql = "
        SELECT app_users_private_data.id,
          username as forum_username, topo_name, url website, email, registered,
          registration_ip, last_visit,
          location, group_id
        FROM app_users_private_data"
      sql += "
        WHERE app_users_private_data.id IN (
          SELECT poster_id FROM punbb_posts WHERE punbb_posts.topic_id = #{@options[:topic]}
        )" if @options[:topic]
      sql += "
        ORDER BY id ASC
        LIMIT #{BATCH_SIZE}
        OFFSET #{offset};"
      results = sql_query(sql)

      break if results.ntuples < 1

      next if all_records_exist? :users, results.map {|u| u["id"].to_i}

      create_users(results, total: total_count, offset: offset) do |user|
        gid = user['group_id'].to_i
        is_staff = (user['group_id'] == 1 || user['group_id'] == 2)
        # puts '', user, ''
        # assuming the login_name is normalized for Discourse
        { id: user['id'],
          email: user['email'],
          username: user['forum_username'], # forum name
          name: user['topo_name'], # full name
          created_at: 0,
          website: user['url'],
          registration_ip_address: user['registered'],
          last_seen_at: Time.zone.at(user['last_visit'].to_i),
          last_emailed_at: 0,
          location: user['location'],
          moderator: user['group_id'] == 2,
          admin: user['group_id'] == 1,
          post_create_action: proc do |newuser|
              if (GROUPS_ASSOCE.include? gid) || is_staff
                group_id = group_id_from_imported_group_id(VIRTUAL_GROUP_ASSOCE_ID)
                GroupUser.find_or_create_by(user: newuser, group_id: group_id)
              end
              if GROUPS_ANCIENS.include? gid
                group_id = group_id_from_imported_group_id(VIRTUAL_GROUP_ANCIENTS_ID)
                GroupUser.find_or_create_by(user: newuser, group_id: group_id)
              end
              if GROUPS_CA.include? gid
                group_id = group_id_from_imported_group_id(VIRTUAL_GROUP_CA_ID)
                GroupUser.find_or_create_by(user: newuser, group_id: group_id)
              end
              if GROUPS_MODOS_TOPO.include? gid
                group_id = group_id_from_imported_group_id(VIRTUAL_GROUP_MODOS_TOPO_ID)
                GroupUser.find_or_create_by(user: newuser, group_id: group_id)
              end
              if GROUPS_PUB.include? gid
                group_id = group_id_from_imported_group_id(VIRTUAL_GROUP_PUB_ID)
                GroupUser.find_or_create_by(user: newuser, group_id: group_id)
              end
              if GROUPS_PARTNERS.include? gid
                group_id = group_id_from_imported_group_id(VIRTUAL_GROUPS_PARTNERS_ID)
                GroupUser.find_or_create_by(user: newuser, group_id: group_id)
              end
          end
        }
      end
    end
  end

  def import_categories
    step "importing top level categories..."

    restricted_categories = [
      6,  # Site et Association
      7,  # Administration des Sites
      12, # Sito e Associazione
      13, # Amministrazioni del sito
      23, # C2C V6
    ]

    categories = sql_query("
      SELECT
        id,
        cat_name,
        disp_position
      FROM punbb_categories
      ORDER BY id ASC").to_a

    create_categories(categories) do |category|

      read_restricted = restricted_categories.include? category["id"].to_i
      suppress_from_homepage = category["id"] == '1' # commentaires topoguide

      {
        id: category["id"],
        name: category["cat_name"],
        read_restricted: read_restricted,
        suppress_from_homepage: suppress_from_homepage
      }
    end

    puts "", "importing children categories..."

    restricted_forums = [
      3,  # Modos Ecotourisme
      36, # Modos Forum
      37, # Modos Topoguide
      40, # Développement
    ]

    forums = sql_query("
      SELECT
        id,
        forum_name,
        forum_desc,
        disp_position,
        cat_id
      FROM punbb_forums
      ORDER BY id").to_a

    restricteds = []
    publics = []
    create_categories(forums) do |forum|

      read_restricted = (
        (restricted_categories.include? forum["cat_id"].to_i) or
        (restricted_forums.include? forum["id"].to_i)
      )
      if read_restricted
        restricteds << forum
      else
        publics << forum
      end

      {
        id: "child##{forum['id']}",
        name: forum["forum_name"],
        read_restricted: read_restricted,
        suppress_from_homepage: false,
        description: forum["forum_desc"],
        parent_category_id: category_id_from_imported_category_id(forum["cat_id"])
      }
    end

    puts "", "restricted forums (#{restricteds.length}):"
    restricteds.each do |forum|
      puts "#{forum["id"]}: #{forum["forum_name"]}"
    end

    puts "", "public forums: (#{publics.length}):"
    publics.each do |forum|
      puts "#{forum["id"]}: #{forum["forum_name"]}"
    end

    puts "", "Add all permissions to group CA"
    group_id = group_id_from_imported_group_id(VIRTUAL_GROUP_CA_ID)
    Category.find_by(:read_restricted == true) do |category|
      CategoryGroup.find_or_create_by(category_id: category.id, group_id: group_id) do |cg|
        cg.permission_type = CategoryGroup.permission_types[:full]
      end
    end
  end

  def create_categories_permalinks
    step "creating categories redirections"

    created = 0
    skipped = 0
    failed = 0

    query = CategoryCustomField.
            joins("LEFT JOIN permalinks ON permalinks.category_id = category_custom_fields.category_id").
            where(name: 'import_id').
            where("url IS NULL")

    total_count = query.count

    query.
        select("category_custom_fields.id, category_custom_fields.category_id, value").
        find_each do |custom_field|

      category_id = custom_field.category_id
      import_id = custom_field.value

      matched = /child#(\d+)/.match(import_id)
      if not matched  # no url to categories in punbb
        skipped += 1
        next
      end
      forum_id = matched[1]

      url = "viewforum.php?id=#{forum_id}"
      if Permalink.where(url: url).exists?
        skipped += 1
        next
      end
      permalink = Permalink.create(url: url, category_id: category_id)
      if permalink
        created += 1
      else
        failed += 1
        puts "Failed to create permalink for forum id: #{forum_id}"
      end

      print_status  created + skipped + failed, total_count
    end
  end

  def update_v5_first_post_id
    # In our old version we do not have t.first_post_id first_post_id
    # https://github.com/punbb/punbb/blob/56e0ca959537adcd44b307d9ed1cb177f9f302f3/admin/db_update.php#L1284-L1307

    step "updating column punbb_topics.first_post_id"

    sql = "
      SELECT count(*) AS count
      FROM information_schema.columns
      WHERE
        table_schema = 'public'
        AND
        table_name = 'punbb_topics'
        AND
        column_name = 'first_post_id';"
    column_count = sql_query(sql).first["count"]
    if column_count.to_s == "0"
      sql_query('ALTER TABLE punbb_topics ADD COLUMN first_post_id integer;')
    end

    # update first_post_id base topic_id and posted (default for all forums)
    sql_query("
      UPDATE punbb_topics
      SET
        first_post_id = calculated_first_post_id
      FROM (
        SELECT *
        FROM (
          SELECT
            topic_id,
            first_value(punbb_posts.id) OVER (PARTITION BY topic_id ORDER BY punbb_posts.posted) AS calculated_first_post_id
          FROM punbb_posts
          ORDER BY topic_id, punbb_posts.posted
        ) AS windowed
        GROUP BY topic_id, calculated_first_post_id
      ) AS grouped
      WHERE grouped.topic_id = punbb_topics.id;
    ")

    # update first_post_id base on forum_id, subject and posted for the topoguide comments
    # as we don't want duplicated topics for the same document
    sql_query("
      UPDATE punbb_topics
      SET first_post_id = calculated_first_post_id
      FROM (
        SELECT *
        FROM (
          SELECT
            topic_id,
            first_value(punbb_posts.id) OVER (PARTITION BY forum_id, subject ORDER BY punbb_posts.posted) AS calculated_first_post_id
          FROM punbb_posts
          LEFT JOIN punbb_topics ON punbb_topics.id = punbb_posts.topic_id
          WHERE forum_id = 1  -- only the topoguide comments
          ORDER BY forum_id, subject, punbb_posts.posted
        ) AS windowed
        GROUP BY topic_id, calculated_first_post_id
      ) AS grouped
      WHERE grouped.topic_id = punbb_topics.id;
    ")
  end

  def import_posts
    update_v5_first_post_id

    step "creating topics and posts"

    sql = "
      SELECT count(*) count
      FROM punbb_posts p
      LEFT JOIN punbb_topics t ON p.topic_id = t.id"
    sql += "
      WHERE topic_id = #{@options[:topic]}" if @options[:topic]
    sql += "
      WHERE t.first_post_id > #{@options[:firstpost]}" if @options[:firstpost]
    total_count = sql_query(sql).first["count"]

    batches(BATCH_SIZE) do |offset|
      start_time = get_start_time("posts-#{total_count}") # the post count should be unique enough to differentiate between posts and PMs
      print_status(offset, total_count, start_time)

      sql = "
        SELECT p.id
        FROM punbb_posts p
        LEFT JOIN punbb_topics t ON p.topic_id = t.id"
      sql += "
        WHERE topic_id = #{@options[:topic]}" if @options[:topic]
      sql += "
        WHERE t.first_post_id > #{@options[:firstpost]}" if @options[:firstpost]
      sql += "
        ORDER BY p.posted
        LIMIT #{BATCH_SIZE} OFFSET #{offset};"
      results = sql_query(sql).to_a

      break if results.size < 1
      next if all_records_exist? :posts, results.map {|u| u["id"].to_i}

      sql = "
        SELECT
          p.id id,
          p.poster poster,
          t.id topic_id,
          t.forum_id category_id,
          t.subject title,
          t.first_post_id,
          p.poster_id user_id,
          p.message raw,
          p.posted created_at,
          f.culture
        FROM punbb_posts p
        LEFT JOIN punbb_topics t ON p.topic_id = t.id
        LEFT JOIN punbb_forums f ON f.id = t.forum_id"
      sql += "
          WHERE p.topic_id = #{@options[:topic]}" if @options[:topic]
      sql += "
          WHERE t.first_post_id > #{@options[:firstpost]}" if @options[:firstpost]
      sql += "
        ORDER BY p.posted
        LIMIT #{BATCH_SIZE} OFFSET #{offset};"
      results = sql_query(sql).to_a

      create_posts(results, total: total_count, offset: offset) do |m|
        skip = false
        mapped = {}

        mapped[:id] = m['id']
        mapped[:user_id] = user_id_from_imported_user_id(m['user_id']) || -1
        mapped[:raw] = process_punbb_post(m['raw'], m['id'])
        if mapped[:user_id] == -1
          ## Prepend the poster name when the poster was anonymous.
          if m['culture'] == 'fr'
            poster_prefix = "Posté en tant qu'invité par"
          else
            poster_prefix = "Posted as guest by"
          end
          mapped[:raw] = "#{poster_prefix} _#{m['poster']}_:\n\n#{mapped[:raw]}"
        end

        mapped[:created_at] = Time.zone.at(m['created_at'].to_i)

        is_first_post = m['id'] == m['first_post_id']
        if is_first_post
          mapped[:category] = category_id_from_imported_category_id("child##{m['category_id']}")
          mapped[:title] = CGI.unescapeHTML(m['title'])

          if m['category_id'] == '1' # Topoguide comments
            # Create a first post with link to document
            import_id = "first_comment_#{m['id']}"
            comment_topic = {}
            comment_topic[:user_id] = -1
            comment_topic[:category] = mapped[:category]
            comment_topic[:title] = mapped[:title]
            comment_topic[:raw] = topoguide_first_post_content(mapped[:title])

            new_post = create_post(comment_topic, import_id)
            if new_post.is_a?(Post)
              @lookup.add_post(import_id, new_post)
              @lookup.add_topic(new_post)
              created_post(new_post)
            else
              puts "Error creating post #{import_id}. Skipping."
              puts new_post.inspect
            end

            is_first_post = false
            m['first_post_id'] = import_id
          end
        end

        if not is_first_post
          parent = topic_lookup_from_imported_post_id(m['first_post_id'])
          if parent
            mapped[:topic_id] = parent[:topic_id]
          else
            puts "Parent post #{m['first_post_id']} doesn't exist. Skipping #{m["id"]}: #{m["title"][0..40]}"
            skip = true
          end
        end

        skip ? nil : mapped
      end
    end
  end

  def topoguide_first_post_content(title)
    tokens = title.split('_')
    document_id = tokens[0].to_i
    return "Document not found" if document_id == 0
    culture = tokens[1]
    sql = "
      SELECT name, module
      FROM app_documents_i18n_archives
      LEFT JOIN app_documents_archives
        ON app_documents_archives.id = app_documents_i18n_archives.id
      WHERE app_documents_i18n_archives.id = #{document_id};"
    app_document = sql_query(sql).first
    return "Document not found" if app_document.nil?

    document_type = app_document["module"]
    if ['summits', 'sites', 'huts', 'access', 'products'].include?(document_type)
      document_type = 'waypoints'
    end
    href = "https://www.camptocamp.org/#{document_type}/#{document_id}/#{culture}"

    "<a href=\"#{href}\">#{app_document['name']}</a>"
  end

  def create_topics_permalinks
    step "creating topics redirections"

    start_time = get_start_time("topic-permalinks")

    sql = "
      SELECT count(*) count
      FROM punbb_topics"
    sql += "
      WHERE id = #{@options[:topic]}" if @options[:topic]
    total_count = sql_query(sql).first["count"]

    puts "total_count: #{total_count}"

    batches(BATCH_SIZE) do |offset|
      created = 0
      skipped = 0
      failed = 0

      sql = "
        SELECT
          id,
          first_post_id
        FROM punbb_topics"
      sql += "
        WHERE id = #{@options[:topic]}" if @options[:topic]
      sql += "
        ORDER BY posted
        LIMIT #{BATCH_SIZE} OFFSET #{offset};"
      results = sql_query(sql).to_a

      break if results.size < 1

      results.each do |result|
        import_id = result['id']
        topic = topic_lookup_from_imported_post_id(result['first_post_id'])
        if !topic
          skipped += 1
          next
        end
        url = "viewtopic.php?id=#{import_id}"
        if Permalink.where(url: url).exists?
          skipped += 1
          next
        end
        permalink = Permalink.create(url: url, topic_id: topic[:topic_id])
        if permalink
          created += 1
        else
          puts "Failed to create permalink for topic id: #{import_id}"
          failed += 1
        end

        print_status created + skipped + failed + (offset || 0), total_count, start_time
      end
    end
  end

  def create_posts_permalinks
    step "creating posts redirections"

    start_time = get_start_time("post-permalinks")

    created = 0
    skipped = 0
    failed = 0

    query = PostCustomField.
        joins("LEFT JOIN permalinks ON permalinks.post_id = post_custom_fields.post_id").
        where(name: 'import_id').
        where("url IS NULL")

    total_count = query.count

    query.
        select("post_custom_fields.id, post_custom_fields.post_id, value").
        find_each do |custom_field|

      post_id = custom_field.post_id
      import_id = custom_field.value

      url = "viewtopic.php?pid=#{import_id}"
      if Permalink.where(url: url).exists?
        skipped += 1
        next
      end
      permalink = Permalink.create(url: url, post_id: post_id)
      if permalink
        created += 1
      else
        failed += 1
        puts "Failed to create permalink for post id: #{import_id}"
      end

      print_status  created + skipped + failed, total_count, start_time
    end
  end

  def suspend_users
    step "updating banned users"

    banned = 0
    failed = 0
    total = sql_query("SELECT count(*) count FROM punbb_bans").first['count']

    system_user = Discourse.system_user

    sql_query("SELECT username, email FROM punbb_bans").each do |b|
      user = User.find_by_email(b['email'])
      if user
        user.suspended_at = Time.now
        user.suspended_till = 200.years.from_now

        if user.save
          StaffActionLogger.new(system_user).log_user_suspend(user, "banned during initial import")
          banned += 1
        else
          puts "Failed to suspend user #{user.username}. #{user.errors.try(:full_messages).try(:inspect)}"
          failed += 1
        end
      else
        puts "Not found: #{b['email']}"
        failed += 1
      end

      print_status banned + failed, total
    end
  end

  def rewriteQuote(quote, post_import_id)
    trimed = quote[7...-1]
    splitted = trimed.split('|')
    return "[quote=\"Citation\"]" if splitted.length < 1
    return quote if splitted.length < 2

    user = splitted[0].gsub('"', '_')
    quote_import_id = splitted[1]
    discourse_id = post_id_from_imported_post_id(quote_import_id)
    return quote if discourse_id.nil?

    post = post_content_from_discourse_post_id(discourse_id)
    "[quote=\"#{user}, id: #{discourse_id}, post:#{post[:post_number]}, topic:#{post[:topic_id]}\"]"
  rescue => e
    # Error, linked post is incorrect or not imported.
    # Keeping the quote as-is.
    puts "Cannot rewrite quote #{quote} in post #{post_import_id}"
    quote
  end

  def rewriteSpoiler(spoiler, post_import_id)
    trimed = spoiler[1...-1]
    splitted = trimed.split('=')
    if splitted.length == 1
      title = '(Cliquez pour afficher)'
    else
      title = splitted[1]
    end

    "<details><summary>#{title}</summary>"
  rescue => e
    # Error, linked post is incorrect or not imported.
    # Keeping the quote as-is.
    puts "Cannot rewrite spoiler #{spoiler} in post #{post_import_id}"
    spoiler
  end

  def process_punbb_post(raw, import_id)
    s = raw.dup

    # error with [url]aide[/url] or [url]association.circuitderando.com[/url]
    s.gsub!(/\[url\](a.*)\[\/url\]/, '[url]http://\1[/url]')

    # Relate https://github.com/c2corg/v6_forum/issues/33
    # transform [img=url][/img] to [img]url[/img]
    s.gsub!(/\[img=([^\]]+)\]\[\/img\]/, '[img]\1[/img]')

    # :) is encoded as <!-- s:) --><img src="{SMILIES_PATH}/icon_e_smile.gif" alt=":)" title="Smile" /><!-- s:) -->
    s.gsub!(/<!-- s(\S+) -->(?:.*)<!-- s(?:\S+) -->/, '\1')

    # Some links look like this: <!-- m --><a class="postlink" href="http://www.onegameamonth.com">http://www.onegameamonth.com</a><!-- m -->
    s.gsub!(/<!-- \w --><a(?:.+)href="(\S+)"(?:.*)>(.+)<\/a><!-- \w -->/, '[\2](\1)')

    # Many phpbb bbcode tags have a hash attached to them. Examples:
    #   [url=https&#58;//google&#46;com:1qh1i7ky]click here[/url:1qh1i7ky]
    #   [quote=&quot;cybereality&quot;:b0wtlzex]Some text.[/quote:b0wtlzex]
    s.gsub!(/:(?:\w{8})\]/, ']')

    # Remove mybb video tags.
    s.gsub!(/(^\[video=.*?\])|(\[\/video\]$)/, '')

    s = CGI.unescapeHTML(s)

    # phpBB shortens link text like this, which breaks our markdown processing:
    #   [http://answers.yahoo.com/question/index ... 223AAkkPli](http://answers.yahoo.com/question/index?qid=20070920134223AAkkPli)
    #
    # Work around it for now:
    s.gsub!(/\[http(s)?:\/\/(www\.)?/, '[')

    # https://github.com/c2corg/v6_forum/issues/33
    s.gsub!(/^\+ 1$/, '+1')

    # [c] => [code] and [/c] => [/code] https://github.com/c2corg/v6_forum/issues/33
    s.gsub!(/\[c\]/, '[code]')
    s.gsub!(/\[\/c\]/, '[/code]')

    # spoilers
    s.gsub!(/\[spoiler[^\]]*\]/) {|spoiler| rewriteSpoiler(spoiler, import_id)}
    s.gsub!(/\[\/spoiler\]/, '</details>')

    # Rewrite quotes: add post number, topic ...
    # [quote=mollotof|2087176] -> [quote="mollotof, id: 2087176, post:23, topic:11892"]
    quote_pattern = /\[quote=[^\]]*\]/
    s.gsub!(quote_pattern) {|quote| rewriteQuote(quote, import_id)}

    s
  end

  def sql_query(sql)
    @client.exec(sql)
  end

  def step(message)
    puts "", "#{Time.now.strftime("%Y-%m-%d %H:%M:%S")} #{message}"
  end
end

if __FILE__ == $0
  ImportScripts::PunBB.new.perform
end
