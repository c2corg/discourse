require "pg"

require File.expand_path(File.dirname(__FILE__) + "/base.rb")

require 'pry'

# Call it like this:
#   RAILS_ENV=production bundle exec ruby script/import_scripts/punbb.rb
class ImportScripts::PunBB < ImportScripts::Base

  PUNBB_DB = "c2corg"
  BATCH_SIZE = 500

  def initialize
    super

    @client = PG::Connection.open(
      :host => "localhost",
      :user => "www-data",
      :password => "www-data",
      :dbname => PUNBB_DB
    )
  end

  def execute
    import_users
    import_categories
    import_posts
    suspend_users
  end

  def normalize_login_name name
    # Discourse has very strict username rules
    # https://meta.discourse.org/t/what-are-the-rules-for-usernames/13458
    name.strip!
    name.gsub!(/[^0-9a-z]/i, '_')
    name.squeeze!('_')
    name.slice! 15..-1 if name.size > 15
    last_char = name[-1, 1]
    name.slice!(-1) if last_char == '_'
  end

  def import_users
    puts '', "creating users"

    total_count = sql_query("SELECT count(*) count FROM users;").first['count']

    batches(BATCH_SIZE) do |offset|
      results = sql_query(
        "SELECT id, login_name, topo_name, url website, email, registered,
                registration_ip, last_visit,
                location, group_id
         FROM app_users_private_data
         ORDER BY id ASC
         LIMIT #{BATCH_SIZE}
         OFFSET #{offset};")

      break if results.ntuples < 1

      next if all_records_exist? :users, results.map {|u| u["id"].to_i}

      create_users(results, total: total_count, offset: offset) do |user|
        # puts '', user, ''
        normalize_login_name(user['login_name'])
        { id: user['id'],
          email: user['email'],
          username: user['login_name'], # login name
          name: user['topo_name'], # full name
          created_at: 0,
          website: user['url'],
          registration_ip_address: user['registered'],
          last_seen_at: Time.zone.at(user['last_visit'].to_i),
          last_emailed_at: 0,
          location: user['location'],
          moderator: user['group_id'] == 4,
          admin: user['group_id'] == 1 }
      end
    end
  end

  def import_categories
    puts "", "importing top level categories..."

    categories = sql_query(
      "SELECT id, cat_name, disp_position
       FROM punbb_categories
       ORDER BY id ASC").to_a

    create_categories(categories) do |category|
      puts category
      {
        id: category["id"],
        name: category["cat_name"]
      }
    end

    puts "", "importing children categories..."

    children_categories = sql_query(
      "SELECT id, forum_name, forum_desc, disp_position, cat_id parent_category_id
       FROM punbb_forums
       ORDER BY id").to_a

    create_categories(children_categories) do |category|
      puts 'subcategory', category
      {
        id: "child##{category['id']}",
        name: category["forum_name"],
        description: category["forum_desc"],
        parent_category_id: category_id_from_imported_category_id(category["parent_category_id"])
      }
    end
  end

  def import_posts
    puts "", "creating topics and posts"

    total_count = sql_query("SELECT count(*) count from punbb_posts").first["count"]

    # In our old version we do not have t.first_post_id first_post_id,
    # ALTER TABLE punbb_topics ADD COLUMN first_post_id integer default 0;
    # UPDATE punbb_topics SET first_post_id = (select MIN(p.id) from punbb_posts as p where punbb_topics.id = p.topic_id);
    # https://github.com/punbb/punbb/blob/56e0ca959537adcd44b307d9ed1cb177f9f302f3/admin/db_update.php#L1284-L1307
    batches(BATCH_SIZE, total_count.to_i * 99.8 / 100) do |offset|
      results = sql_query("
        SELECT id FROM punbb_posts ORDER BY posted
        LIMIT #{BATCH_SIZE} OFFSET #{offset};
      ").to_a

      break if results.size < 1
      next if all_records_exist? :posts, results.map {|u| u["id"].to_i}

      results = sql_query("
        SELECT p.id id,
               p.poster poster,
               t.id topic_id,
               t.forum_id category_id,
               t.subject title,
               t.first_post_id,
               p.poster_id user_id,
               p.message raw,
               p.posted created_at
        FROM punbb_posts p,
             punbb_topics t
        WHERE p.topic_id = t.id
        ORDER BY p.posted
        LIMIT #{BATCH_SIZE} OFFSET #{offset};
      ").to_a

      create_posts(results, total: total_count, offset: offset) do |m|
        skip = false
        mapped = {}

        mapped[:id] = m['id']
        mapped[:user_id] = user_id_from_imported_user_id(m['user_id']) || -1
        mapped[:raw] = process_punbb_post(m['raw'], m['id'])
        if mapped[:user_id] == -1
          ## Prepend the poster name when the poster was anonymous.
          mapped[:raw] = "Posted as guest by _#{m['poster']}_:\n#{mapped[:raw]}"
        end

        mapped[:created_at] = Time.zone.at(m['created_at'].to_i)

        # Force id to be the same as import_id
        mapped[:forced_id] = m['id']

        if m['id'] == m['first_post_id']
          mapped[:category] = category_id_from_imported_category_id("child##{m['category_id']}")
          mapped[:title] = CGI.unescapeHTML(m['title'])
        else
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

  def suspend_users
    puts '', "updating banned users"

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

  def rewriteQuote(quote)
    trimed = quote[7...-1]
    splitted = trimed.split('|')
    user = splitted[0].gsub('"', '_')
    import_id = splitted[1]

    discourse_id = post_id_from_imported_post_id(import_id)
    post = post_content_from_discourse_post_id(discourse_id)

    "[quote=\"#{user}, id: #{discourse_id}, post:#{post[:post_number]}, topic:#{post[:topic_id]}\"]"
  rescue => e
    # Error, linked post is incorrect or not imported.
    # Keeping the quote as-is.
    puts "Cannot rewrite quote #{quote}"
    quote
  end


  def process_punbb_post(raw, import_id)
    s = raw.dup

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

    # Rewrite quotes: add post number, topic ...
    # [quote=mollotof|2087176] -> [quote="mollotof, id: 2087176, post:23, topic:11892"]
    quote_pattern = /\[quote=[^\]]*\]/
    s.gsub!(quote_pattern) {|quote| rewriteQuote(quote)}

    s
  end

  def sql_query(sql)
    @client.exec(sql)
  end
end

if __FILE__ == $0
  ImportScripts::PunBB.new.perform
end
