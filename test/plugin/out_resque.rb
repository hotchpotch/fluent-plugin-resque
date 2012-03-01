require 'test_helper'
require 'resque'

class ResqueOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    require 'fluent/plugin/out_resque'
  end

  CONFIG = %[
    type resque
    queue test_queue
    time_format %y-%m-%d %H:%M:%S
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::ResqueOutput) {
    }.configure(conf)
  end

  def test_write
    d = create_driver
    time = Time.at(Time.now.to_i).utc
    d.emit({'a' => 1}, time)
    d.emit({'b' => 2}, time)
    mock(Resque).enqueue_to("test_queue", "Test", {"a" => 1 , "time" => time.strftime("%y-%m-%d %H:%M:%S")})
    mock(Resque).enqueue_to("test_queue", "Test", {"b" => 2 , "time" => time.strftime("%y-%m-%d %H:%M:%S")})
    d.run
  end

  def test_write_except_time_key
    d = create_driver(CONFIG + "\ninclude_time_key false")
    time = Time.at(Time.now.to_i).utc
    d.emit({'a' => 1}, time)
    mock(Resque).enqueue_to("test_queue", "Test", {"a" => 1})
    d.run
  end

  def test_write_include_tag_key
    d = create_driver(CONFIG + "\ninclude_tag_key true")
    time = Time.at(Time.now.to_i).utc
    d.emit({'a' => 1}, time)
    mock(Resque).enqueue_to("test_queue", "Test", {"a" => 1, "time" => time.strftime("%y-%m-%d %H:%M:%S"), "tag" => 'test'})
    d.run
  end

  def test_write_with_remove_tag_prefix
    d = create_driver(CONFIG + "\nremove_tag_prefix te")
    time = Time.at(Time.now.to_i).utc
    d.emit({'a' => 1}, time)
    mock(Resque).enqueue_to("test_queue", "St", {"a" => 1, "time" => time.strftime("%y-%m-%d %H:%M:%S")})
    d.run
  end

  def test_write_add_tag_prefix
    d = create_driver(CONFIG + %[
      add_tag_prefix worker.
      remove_tag_prefix t
    ])
    time = Time.at(Time.now.to_i).utc
    d.emit({'a' => 1}, time)
    mock(Resque).enqueue_to("test_queue", "Worker::Est", {"a" => 1, "time" => time.strftime("%y-%m-%d %H:%M:%S")})
    d.run
  end

  def test_collect
    d = create_driver(CONFIG + %[
      collect true
    ])
    time = Time.at(Time.now.to_i).utc
    d.emit({'a' => 1}, time)
    d.emit({'b' => 2}, time)
    mock(Resque).enqueue_to("test_queue", "Test", [
      {"a" => 1, "time" => time.strftime("%y-%m-%d %H:%M:%S")}, {"b" => 2, "time" => time.strftime("%y-%m-%d %H:%M:%S")}
    ])
    d.run
  end

  def test_change_redis_host
    mock(Resque).redis = "localhost:11111/namespace"
    d = create_driver(CONFIG + "\nredis localhost:11111/namespace")
  end
end
