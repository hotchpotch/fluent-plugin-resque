
module Fluent
  class ResqueOutput < BufferedOutput
    Fluent::Plugin.register_output('resque', self)

    include SetTagKeyMixin
    config_set_default :include_tag_key, false

    include SetTimeKeyMixin
    config_set_default :include_time_key, true

    include HandleTagNameMixin

    config_param :queue, :string
    config_param :redis, :string, :default => nil

    def initialize
      super
      require 'multi_json'
      require 'redis'
    end

    def configure(conf)
      super

      redis = conf['redis'] if conf['redis']
    end

    # code from resque.rb
    def redis=(server)
      case server
      when String
        if server =~ /redis\:\/\//
          redis = Redis.connect(:url => server, :thread_safe => true)
        else
          server, namespace = server.split('/', 2)
          host, port, db = server.split(':')
          redis = Redis.new(:host => host, :port => port,
                            :thread_safe => true, :db => db)
        end
        namespace ||= :resque

        @redis = Redis::Namespace.new(namespace, :redis => redis)
      when Redis::Namespace
        @redis = server
      else
        @redis = Redis::Namespace.new(:resque, :redis => server)
      end
    end

    def redis
      return @redis if @redis
      self.redis = Redis.respond_to?(:connect) ? Redis.connect : "localhost:6379"
      self.redis
    end

    def enqueue(queue, klass, args)
      redis.sadd(:queues, queue.to_s)
      redis.rpush(queue, ::MultiJson.encode(:class => klass, :args => args))
    end

    def start
      super
    end

    def shutdown
      super
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      queue_name = @queue_mapped ? chunk.key : @queue

      chunk.msgpack_each {|tag, time, record|
        enqueue(queue_name, camelize(tag), record) 
      }
    end

    private
    def remove_prefix(tag)
      tag.to_s.sub(@remove_tag_prefix, '')
    end

    def camelize(name)
      name.to_s.gsub(/\.(.?)/) { "::" + $1.upcase }.gsub(/(^|_)(.)/) { $2.upcase }
    end
  end
end
