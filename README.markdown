# es-reindex - simple ruby script for copying ElasticSearch index

Simple ruby script to copy and reindex ElasticSearch index,
copying index settings and mapping(s).

Progress and time estimation is displayed during the scrolling process.

## Requirements

Following gems are required via `Gemfile`:

+ [rest-client] (https://github.com/archiloque/rest-client)
+ [yajl-ruby] (https://github.com/brianmario/yajl-ruby)

You can install the requirements locally via `bundler`:

    $ bundle install --path=.bundle

## Usage

Refer to script's help:

    $ ./es-reindex.rb -h
    
    Script to copy particular ES index including its (re)creation w/options set
    and mapping copied.
    
    Usage:
    
      ./es-reindex.rb [-r] [-f <frame>] [source_url/]<index> [destination_url/]<index>
    
        - -r - remove the index in the new location first
        - -f - specify frame size to be obtained with one fetch during scrolling
        - optional source/destination urls default to http://127.0.0.1:9200


## Changelog

+ __0.0.1__: first revision

## License

es-reindex is copyright (c)2012 Jiri Nemecek, and released under the terms
of the MIT license. See the LICENSE file for the gory details.

