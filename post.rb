require_relative "script/import_scripts/punbb"
require 'pry'

# This tool allows to test the conversion of a post message from
# c2c/punbb to discourse.
# It is called with the post id, which can be easily extracted from forum urls like
# http://www.camptocamp.org/forums/viewtopic.php?pid=1013031#p1013031
#
# The post message can be displayed: raw, after simple transformations in punbb.rb,
# after conversion to markdown.
#
# In order to check the markdown conversion is "correct", the unix command 'markdown'
# is used to convert from markdown to html. The resulting file: post.html can be opened
# in a regular browser.
#
# CF forum discussions about bbcode in http://www.camptocamp.org/forums/viewtopic.php?id=139585


if ARGV.length < 1
  puts "usage: <postid>"
  exit
end

importer = ImportScripts::PunBB.new

postid = ARGV[0]

raw = importer.sql_query("SELECT id, message FROM punbb_posts where id = #{postid}").first['message']
processed = importer.process_punbb_post(raw, nil)
md = processed.bbcode_to_md

File.open('post.raw', 'w') { |file| file.write(raw) }
File.open('post.simple', 'w') { |file| file.write(processed) }
File.open('post.md', 'w') { |file| file.write(md) }

# default / markdown
puts '=== Markdown =='
puts md
utf8_header = '
<head>
  <meta charset="UTF-8">
</head>
'

`echo '#{utf8_header}' > post.html`
`markdown post.md >> post.html` 
