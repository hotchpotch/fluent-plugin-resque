
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
    config_param :collect, :bool, :default => false

    def initialize
      super
      require 'resque'
    end

    def configure(conf)
      super

      Resque.redis = conf['redis'] if conf['redis']
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

      if @collect
        records = Hash.new
        chunk.msgpack_each {|tag, time, record|
          (records[camelize(tag)] ||= []) << record
        }
        records.each {|camelize_tag, record|
          Resque.enqueue_to(queue_name, camelize_tag, record) 
        }
      else
        chunk.msgpack_each {|tag, time, record|
          Resque.enqueue_to(queue_name, camelize(tag), record) 
        }
      end
    end

    private

    def camelize(name)
      name.to_s.gsub(/\.(.?)/) { "::" + $1.upcase }.gsub(/(^|_)(.)/) { $2.upcase }
    end
  end
end
