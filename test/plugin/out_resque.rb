require 'test_helper'
require 'fluent/plugin/out_resque'
require 'multi_json'

class ResqueOutputTest < Test::Unit::TestCase
  def setup
    super
    Fluent::Test.setup
    @subject = Object.new
    any_instance_of(Fluent::ResqueOutput, :redis= => lambda {}, :redis => @subject)
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

  def check_enqueue(queue, klass, args)
    mock(@subject).sadd(:queues, "test_queue").any_times
    mock(@subject).rpush("queue:#{queue}", ::MultiJson.encode(:class => klass, :args => [args]))
  end

  def test_write
    d = create_driver
    time = Time.at(Time.now.to_i).utc
    d.emit({'a' => 1, "class" => "WorkerTest"}, time)
    d.emit({'b' => 2, "class" => "WorkerTest"}, time)
    check_enqueue("test_queue", "WorkerTest", {"a" => 1, "time" => time.strftime("%y-%m-%d %H:%M:%S")})
    check_enqueue("test_queue", "WorkerTest", {"b" => 2, "time" => time.strftime("%y-%m-%d %H:%M:%S")})
    d.run
    assert_equal true, true
  end

  def test_write_except_time_key
    d = create_driver(CONFIG + "\ninclude_time_key false")
    time = Time.at(Time.now.to_i).utc
    d.emit({'a' => 1, 'class' => 'WorkerTest'}, time)
    check_enqueue("test_queue", "WorkerTest", {"a" => 1})
    d.run
  end

  def test_write_include_tag_key
    d = create_driver(CONFIG + "\ninclude_tag_key true")
    time = Time.at(Time.now.to_i).utc
    d.emit({'a' => 1, 'class' => 'WorkerTest'}, time)
    check_enqueue("test_queue", "WorkerTest", {"a" => 1, "tag" => 'test', "time" => time.strftime("%y-%m-%d %H:%M:%S")})
    d.run
  end

  def test_write_change_worker_class_name_tag
    d = create_driver(CONFIG + "\nworker_class_name_tag klass")
    time = Time.at(Time.now.to_i).utc
    d.emit({'a' => 1, 'klass' => 'WorkerTest::Test'}, time)
    check_enqueue("test_queue", "WorkerTest::Test", {"a" => 1, "time" => time.strftime("%y-%m-%d %H:%M:%S")})
    d.run
  end
end
