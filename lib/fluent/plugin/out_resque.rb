
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
      require 'resque'
    end

    def configure(conf)
      super

      Resque.redis = conf['redis'] if conf['redis']

      if remove_tag_prefix = conf['remove_tag_prefix']
        @remove_tag_prefix = Regexp.new('^' + Regexp.escape(remove_tag_prefix))
      end
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
        Resque.enqueue_to(queue_name, camelize(tag), record) 
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
