#!/usr/bin/env ruby
#encoding:utf-8
require 'rubygems'
require 'bundler/setup'
require 'rest-client'
require 'oj'

VERSION = '0.0.8'

STDOUT.sync = true

if ARGV.size == 0 or ARGV[0] =~ /^-(?:h|-?help)$/
  puts "
Script to copy particular ES index including its (re)creation w/options set
and mapping copied.

Usage:

  #{__FILE__} [-r] [-f <frame>] [source_url/]<index> [destination_url/]<index>

    - -r - remove the index in the new location first
    - -f - specify frame size to be obtained with one fetch during scrolling
    - -u - update existing documents (default: only create non-existing)
    - optional source/destination urls default to http://127.0.0.1:9200
\n"
  exit 1
end

Oj.default_options = {:mode => :compat}

remove, update, frame, src, dst = false, false, 1000, nil, nil

while ARGV[0]
  case arg = ARGV.shift
  when '-r' then remove = true
  when '-f' then frame = ARGV.shift.to_i
  when '-u' then update = true
  else
    u = arg.chomp '/'
    !src ? (src = u) : !dst ? (dst = u) :
      raise("Unexpected parameter '#{arg}'. Use '-h' for help.")
  end
end

surl, durl, sidx, didx = '', '', '', ''
[[src, surl, sidx], [dst, durl, didx]].each do |param, url, idx|
  if param =~ %r{^(.*)/(.*?)$}
    url.replace $1
    idx.replace $2
  else
    url.replace 'http://127.0.0.1:9200'
    idx.replace param
  end
end
printf "Copying '%s/%s' to '%s/%s'%s\n  Confirm or hit Ctrl-c to abort...\n",
  surl, sidx, durl, didx,
  remove ?
    ' with rewriting destination mapping!' :
    update ? ' with updating existing documents!' : '.'

$stdin.readline

def tm_len l
  t = []
  t.push l/86400; l %= 86400
  t.push l/3600;  l %= 3600
  t.push l/60;    l %= 60
  t.push l
  out = sprintf '%u', t.shift
  out = out == '0' ? '' : out + ' days, '
  out << sprintf('%u:%02u:%02u', *t)
  out
end

def retried_request method, url, data=nil
  while true
    begin
      return data ?
        RestClient.send(method, url, data) :
        RestClient.send(method, url)
    rescue RestClient::ResourceNotFound # no point to retry
      return nil
    rescue => e
      warn "\nRetrying #{method.to_s.upcase} ERROR: #{e.class} - #{e.message}"
      warn e.response
    end
  end
end

# since scroll API differs in different ES versions
# here is different scroll according to version
def scroll_request(url, scroll_id, es_version='1.5')
  if ( es_version.split('.') <=> ['5'] ) >= 0
    payload = Oj.dump({ scroll_id: scroll_id})
  else
    payload = scroll_id
  end
  while true
    begin
      data = RestClient::Request.execute(
        method: :get,
        url: url,
        payload: payload)
      return data
    rescue RestClient::ResourceNotFound # no point to retry
      return nil
    rescue => e
      warn "\nRetrying scroll #{url} scroll_id #{scroll_id} ERROR: #{e.class} - #{e.message}"
      warn e.response
    end
  end
end

# remove old index in case of remove=true
retried_request(:delete, "#{durl}/#{didx}") \
  if remove && retried_request(:get, "#{durl}/#{didx}/_recovery")

# (re)create destination index
unless retried_request(:get, "#{durl}/#{didx}/_recovery")
  # obtain the original index settings first
  unless settings = retried_request(:get, "#{surl}/#{sidx}/_settings")
    warn "Failed to obtain original index '#{surl}/#{sidx}' settings!"
    exit 1
  end
  settings = Oj.load settings
  sidx = settings.keys[0]
  settings[sidx].delete 'index.version.created'
  printf 'Creating \'%s/%s\' index with settings from \'%s/%s\'... ',
      durl, didx, surl, sidx
  unless retried_request(:post, "#{durl}/#{didx}", Oj.dump(settings[sidx]))
    puts 'FAILED!'
    exit 1
  else
    puts 'OK.'
  end
  unless mappings = retried_request(:get, "#{surl}/#{sidx}/_mapping")
    warn "Failed to obtain original index '#{surl}/#{sidx}' mappings!"
    exit 1
  end
  mappings = Oj.load mappings
  mappings[sidx]['mappings'].each_pair{|type, mapping|
    printf 'Copying mapping \'%s/%s/%s\'... ', durl, didx, type
    unless retried_request(:put, "#{durl}/#{didx}/#{type}/_mapping",
        Oj.dump({type => mapping}))
      puts 'FAILED!'
      exit 1
    else
      puts 'OK.'
    end
  }
end

printf "Copying '%s/%s' to '%s/%s'... \n", surl, sidx, durl, didx
t, done = Time.now, 0
shards = retried_request :get, "#{surl}/#{sidx}/_count?q=*"
shards = Oj.load(shards)['_shards']['total'].to_i

# we need to know ES version to be able to make correct search
# search_type=scan was replaced with sort=_doc for ES > 5.0
# it was deprecated starting from ES 2.1
# (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-request-search-type.html#scan)
es_version = retried_request :get, "#{surl}"
es_version = Oj.load(es_version)['version']['number']

if ( es_version.split('.') <=> ['5'] ) >= 0
  printf "Source ElasticSearch has version %s. Using sort=_doc\n", es_version
  scan = retried_request(:get, "#{surl}/#{sidx}/_search" +
    "?sort=_doc&scroll=10m&size=#{frame / shards}")
else
  printf "Source ElasticSearch has version %s. Using search_type=scan\n", es_version
  scan = retried_request(:get, "#{surl}/#{sidx}/_search" +
    "?search_type=scan&scroll=10m&size=#{frame / shards}")
end
scan = Oj.load scan
scroll_id = scan['_scroll_id']
total = scan['hits']['total']
printf "    %u/%u (%.1f%%) done.\r", done, total, 0

bulk_op = update ? 'index' : 'create'

data = scan
while true do
  unless data['hits']['hits'].empty?
    # this is when ElasticSearch uses sort=_doc
    # first request already returns data
    bulk = ''
    data['hits']['hits'].each do |doc|
      ### === implement possible modifications to the document
      ### === end modifications to the document
      base = {'_index' => didx, '_id' => doc['_id'], '_type' => doc['_type']}
      ['_timestamp', '_ttl'].each{|doc_arg|
        base[doc_arg] = doc[doc_arg] if doc.key? doc_arg
      }
      bulk << Oj.dump({bulk_op => base}) + "\n"
      bulk << Oj.dump(doc['_source']) + "\n"
      done += 1
    end
    bulk << "\n" # empty line in the end required
    retried_request :post, "#{durl}/_bulk", bulk

    eta = total * (Time.now - t) / done
    printf "    %u/%u (%.1f%%) done in %s, E.T.A.: %s.\r",
    done, total, 100.0 * done / total, tm_len(Time.now - t), t + eta
  end
  data = scroll_request("#{surl}/_search/scroll?scroll=10m", scroll_id, es_version)
  data = Oj.load data
  break if data['hits']['hits'].empty?
  scroll_id = data['_scroll_id']
end

printf "#{' ' * 80}\r    %u/%u done in %s.\n",
  done, total, tm_len(Time.now - t)

# no point for large reindexation with data still being stored in index
printf 'Checking document count... '
scount, dcount = 1, 0
begin
  Timeout::timeout(60) do
    while true
      scount = retried_request :get, "#{surl}/#{sidx}/_count?q=*"
      dcount = retried_request :get, "#{durl}/#{didx}/_count?q=*"
      scount = Oj.load(scount)['count'].to_i
      dcount = Oj.load(dcount)['count'].to_i
      break if scount == dcount
      sleep 1
    end
  end
rescue Timeout::Error
end
printf "%u == %u (%s\n",
  scount, dcount, scount == dcount ? 'equals).' : 'NOT EQUAL)!'

exit 0
