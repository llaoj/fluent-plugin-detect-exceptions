# Copyright 2016 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'flexmock/test_unit'
require_relative '../helper'
require 'fluent/plugin/out_detect_exceptions'
require 'json'

class DetectExceptionsOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = <<~END_CONFIG.freeze
    remove_tag_prefix prefix
  END_CONFIG

  DEFAULT_TAG = 'prefix.test.tag'.freeze

  DEFAULT_TAG_STRIPPED = 'test.tag'.freeze

  ARBITRARY_TEXT = 'This line is not an exception.'.freeze

  JAVA_EXC = <<~END_JAVA.freeze
    SomeException: foo
      at bar
    Caused by: org.AnotherException
      at bar2
      at bar3
  END_JAVA

  JAVA_MYSQL_EXC = <<~END_JAVA_MYSQL.freeze
    org.springframework.dao.DataIntegrityViolationException:
    ### Error updating database.  Cause: com.mysql.cj.jdbc.exceptions.MysqlDataTruncation: Data truncation: Out of range value for column 'shutdown_time' at row 1
    ### The error may exist in com/cosmo/hmzy/dao/TShutdownRecordDayMapper.java (best guess)
    ### The error may involve com.cosmo.hmzy.dao.TShutdownRecordDayMapper.insert-Inline
    ### The error occurred while setting parameters
    ### SQL: INSERT INTO t_shutdown_record_day  ( equipment_no, shutdown_start_time,  shutdown_time, budget_clamping_time, shutdown_exception_time, shutdown_status, closeloop_status,                             data_node, delete_flag, create_time,  update_time,  operator_flag, foreman_flag, process_head_flag, manufacture_head_flag, chain_group_leader_flag, t_shutdown_record_id, last_record_id )  VALUES  ( ?, ?,  ?, ?, ?, ?, ?,                             ?, ?, ?,  ?,  ?, ?, ?, ?, ?, ?, ? )
    ### Cause: com.mysql.cj.jdbc.exceptions.MysqlDataTruncation: Data truncation: Out of range value for column 'shutdown_time' at row 1
    ; Data truncation: Out of range value for column 'shutdown_time' at row 1; nested exception is com.mysql.cj.jdbc.exceptions.MysqlDataTruncation: Data truncation: Out of range value for column 'shutdown_time' at row 1
            at org.springframework.jdbc.support.SQLStateSQLExceptionTranslator.doTranslate(SQLStateSQLExceptionTranslator.java:104) ~[spring-jdbc-5.2.15.RELEASE.jar!/:5.2.15.RELEASE]
            at org.springframework.jdbc.support.AbstractFallbackSQLExceptionTranslator.translate(AbstractFallbackSQLExceptionTranslator.java:72) ~[spring-jdbc-5.2.15.RELEASE.jar!/:5.2.15.RELEASE]
            at org.springframework.jdbc.support.AbstractFallbackSQLExceptionTranslator.translate(AbstractFallbackSQLExceptionTranslator.java:81) ~[spring-jdbc-5.2.15.RELEASE.jar!/:5.2.15.RELEASE]
            at org.springframework.jdbc.support.AbstractFallbackSQLExceptionTranslator.translate(AbstractFallbackSQLExceptionTranslator.java:81) ~[spring-jdbc-5.2.15.RELEASE.jar!/:5.2.15.RELEASE]
            at org.mybatis.spring.MyBatisExceptionTranslator.translateExceptionIfPossible(MyBatisExceptionTranslator.java:73) ~[mybatis-spring-2.0.1.jar!/:2.0.1]
            at org.mybatis.spring.SqlSessionTemplate$SqlSessionInterceptor.invoke(SqlSessionTemplate.java:446) ~[mybatis-spring-2.0.1.jar!/:2.0.1]
            at com.sun.proxy.$Proxy113.insert(Unknown Source) ~[?:?]
            at org.mybatis.spring.SqlSessionTemplate.insert(SqlSessionTemplate.java:278) ~[mybatis-spring-2.0.1.jar!/:2.0.1]
            at com.baomidou.mybatisplus.core.override.MybatisMapperMethod.execute(MybatisMapperMethod.java:58) ~[mybatis-plus-core-3.1.2.jar!/:3.1.2]
            at com.baomidou.mybatisplus.core.override.MybatisMapperProxy.invoke(MybatisMapperProxy.java:62) ~[mybatis-plus-core-3.1.2.jar!/:3.1.2]
            at com.sun.proxy.$Proxy149.insert(Unknown Source) ~[?:?]
            at com.cosmo.hmzy.mqtt.factory.impl.StandardDataMqttHandler.insertRecordDayWithStatusShutdown(StandardDataMqttHandler.java:296) ~[classes!/:1.0-SNAPSHOT]
            at com.cosmo.hmzy.mqtt.factory.impl.StandardDataMqttHandler.handle(StandardDataMqttHandler.java:148) ~[classes!/:1.0-SNAPSHOT]
            at com.cosmo.hmzy.service.MqttServiceImpl.handle(MqttServiceImpl.java:22) ~[classes!/:1.0-SNAPSHOT]
            at com.cosmo.hmzy.mqtt.MqttShutdownRecordConfiguration.lambda$shutdownRecordHandler$37(MqttShutdownRecordConfiguration.java:86) ~[classes!/:1.0-SNAPSHOT]
            at org.springframework.integration.handler.ReplyProducingMessageHandlerWrapper.handleRequestMessage(ReplyProducingMessageHandlerWrapper.java:58) [spring-integration-core-5.3.8.RELEASE.jar!/:5.3.8.RELEASE]
            at org.springframework.integration.handler.AbstractReplyProducingMessageHandler.handleMessageInternal(AbstractReplyProducingMessageHandler.java:134) [spring-integration-core-5.3.8.RELEASE.jar!/:5.3.8.RELEASE]
            at org.springframework.integration.handler.AbstractMessageHandler.handleMessage(AbstractMessageHandler.java:62) [spring-integration-core-5.3.8.RELEASE.jar!/:5.3.8.RELEASE]
            at org.springframework.integration.dispatcher.AbstractDispatcher.tryOptimizedDispatch(AbstractDispatcher.java:115) [spring-integration-core-5.3.8.RELEASE.jar!/:5.3.8.RELEASE]
            at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:133) [spring-integration-core-5.3.8.RELEASE.jar!/:5.3.8.RELEASE]
            at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:106) [spring-integration-core-5.3.8.RELEASE.jar!/:5.3.8.RELEASE]
            at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:72) [spring-integration-core-5.3.8.RELEASE.jar!/:5.3.8.RELEASE]
            at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:570) [spring-integration-core-5.3.8.RELEASE.jar!/:5.3.8.RELEASE]
            at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:520) [spring-integration-core-5.3.8.RELEASE.jar!/:5.3.8.RELEASE]
            at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:187) [spring-messaging-5.2.15.RELEASE.jar!/:5.2.15.RELEASE]
            at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:166) [spring-messaging-5.2.15.RELEASE.jar!/:5.2.15.RELEASE]
            at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:47) [spring-messaging-5.2.15.RELEASE.jar!/:5.2.15.RELEASE]
            at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:109) [spring-messaging-5.2.15.RELEASE.jar!/:5.2.15.RELEASE]
            at org.springframework.integration.endpoint.MessageProducerSupport.sendMessage(MessageProducerSupport.java:208) [spring-integration-core-5.3.8.RELEASE.jar!/:5.3.8.RELEASE]
            at org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter.messageArrived(MqttPahoMessageDrivenChannelAdapter.java:391) [spring-integration-mqtt-5.3.8.RELEASE.jar!/:5.3.8.RELEASE]
            at org.eclipse.paho.client.mqttv3.internal.CommsCallback.deliverMessage(CommsCallback.java:519) [org.eclipse.paho.client.mqttv3-1.2.4.jar!/:?]
            at org.eclipse.paho.client.mqttv3.internal.CommsCallback.handleMessage(CommsCallback.java:417) [org.eclipse.paho.client.mqttv3-1.2.4.jar!/:?]
            at org.eclipse.paho.client.mqttv3.internal.CommsCallback.run(CommsCallback.java:214) [org.eclipse.paho.client.mqttv3-1.2.4.jar!/:?]
            at java.lang.Thread.run(Thread.java:750) [?:1.8.0_362]
    Caused by: com.mysql.cj.jdbc.exceptions.MysqlDataTruncation: Data truncation: Out of range value for column 'shutdown_time' at row 1
            at com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping.translateException(SQLExceptionsMapping.java:104) ~[mysql-connector-java-8.0.11.jar!/:8.0.11]
            at com.mysql.cj.jdbc.ClientPreparedStatement.executeInternal(ClientPreparedStatement.java:960) ~[mysql-connector-java-8.0.11.jar!/:8.0.11]
            at com.mysql.cj.jdbc.ClientPreparedStatement.execute(ClientPreparedStatement.java:388) ~[mysql-connector-java-8.0.11.jar!/:8.0.11]
            at com.alibaba.druid.pool.DruidPooledPreparedStatement.execute(DruidPooledPreparedStatement.java:483) ~[druid-1.2.18.jar!/:?]
            at sun.reflect.GeneratedMethodAccessor147.invoke(Unknown Source) ~[?:?]
            at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_362]
            at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_362]
            at org.apache.ibatis.logging.jdbc.PreparedStatementLogger.invoke(PreparedStatementLogger.java:59) ~[mybatis-3.5.1.jar!/:3.5.1]
            at com.sun.proxy.$Proxy219.execute(Unknown Source) ~[?:?]
            at org.apache.ibatis.executor.statement.PreparedStatementHandler.update(PreparedStatementHandler.java:47) ~[mybatis-3.5.1.jar!/:3.5.1]
            at org.apache.ibatis.executor.statement.RoutingStatementHandler.update(RoutingStatementHandler.java:74) ~[mybatis-3.5.1.jar!/:3.5.1]
            at sun.reflect.GeneratedMethodAccessor221.invoke(Unknown Source) ~[?:?]
            at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_362]
            at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_362]
            at org.apache.ibatis.plugin.Plugin.invoke(Plugin.java:63) ~[mybatis-3.5.1.jar!/:3.5.1]
            at com.sun.proxy.$Proxy217.update(Unknown Source) ~[?:?]
            at com.baomidou.mybatisplus.core.executor.MybatisSimpleExecutor.doUpdate(MybatisSimpleExecutor.java:54) ~[mybatis-plus-core-3.1.2.jar!/:3.1.2]
            at org.apache.ibatis.executor.BaseExecutor.update(BaseExecutor.java:117) ~[mybatis-3.5.1.jar!/:3.5.1]
            at sun.reflect.GeneratedMethodAccessor220.invoke(Unknown Source) ~[?:?]
            at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_362]
            at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_362]
            at org.apache.ibatis.plugin.Plugin.invoke(Plugin.java:63) ~[mybatis-3.5.1.jar!/:3.5.1]
            at com.sun.proxy.$Proxy216.update(Unknown Source) ~[?:?]
            at org.apache.ibatis.session.defaults.DefaultSqlSession.update(DefaultSqlSession.java:197) ~[mybatis-3.5.1.jar!/:3.5.1]
            at org.apache.ibatis.session.defaults.DefaultSqlSession.insert(DefaultSqlSession.java:184) ~[mybatis-3.5.1.jar!/:3.5.1]
            at sun.reflect.GeneratedMethodAccessor1646.invoke(Unknown Source) ~[?:?]
            at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_362]
            at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_362]
            at org.mybatis.spring.SqlSessionTemplate$SqlSessionInterceptor.invoke(SqlSessionTemplate.java:433) ~[mybatis-spring-2.0.1.jar!/:2.0.1]
            ... 28 more
  END_JAVA_MYSQL

  PHP_EXC = <<~END_PHP.freeze
    exception 'Exception' with message 'Custom exception' in /home/joe/work/test-php/test.php:5
    Stack trace:
    #0 /home/joe/work/test-php/test.php(9): func1()
    #1 /home/joe/work/test-php/test.php(13): func2()
    #2 {main}
  END_PHP

  PYTHON_EXC = <<~END_PYTHON.freeze
    Traceback (most recent call last):
      File "/base/data/home/runtimes/python27/python27_lib/versions/third_party/webapp2-2.5.2/webapp2.py", line 1535, in __call__
        rv = self.handle_exception(request, response, e)
    Exception: ('spam', 'eggs')
  END_PYTHON

  RUBY_EXC = <<~END_RUBY.freeze
    examble.rb:18:in `thrower': An error has occurred. (RuntimeError)
      from examble.rb:14:in `caller'
      from examble.rb:10:in `helper'
      from examble.rb:6:in `writer'
      from examble.rb:2:in `runner'
      from examble.rb:21:in `<main>'
  END_RUBY

  def create_driver(conf = CONFIG, tag = DEFAULT_TAG)
    d = Fluent::Test::OutputTestDriver.new(Fluent::DetectExceptionsOutput, tag)
    d.configure(conf)
    d
  end

  def log_entry(message, count, stream)
    log_entry = { 'message' => message, 'count' => count }
    log_entry['stream'] = stream unless stream.nil?
    log_entry
  end

  def feed_lines_without_line_breaks(driver, timestamp, *messages, stream: nil)
    count = 0
    messages.each do |m|
      m.each_line do |line|
        line.delete!("\n")
        driver.emit(log_entry(line, count, stream), timestamp + count)
        count += 1
      end
    end
  end

  def feed_lines(driver, timestamp, *messages, stream: nil)
    count = 0
    messages.each do |m|
      m.each_line do |line|
        driver.emit(log_entry(line, count, stream), timestamp + count)
        count += 1
      end
    end
  end

  def run_driver(driver, *messages)
    t = Time.now.to_i
    driver.run do
      feed_lines(driver, t, *messages)
    end
  end

  def make_logs(timestamp, *messages, stream: nil)
    count = 0
    logs = []
    messages.each do |m|
      logs << [timestamp + count, log_entry(m, count, stream)]
      count += m.lines.count
    end
    logs
  end

  def test_configure
    assert_nothing_raised do
      create_driver
    end
  end

  def test_exception_detection
    d = create_driver
    t = Time.now.to_i
    messages = [ARBITRARY_TEXT, JAVA_EXC, ARBITRARY_TEXT]
    d.run do
      feed_lines(d, t, *messages)
    end
    assert_equal(make_logs(t, *messages), d.events)
  end

  def test_mysql_exception_detection
    d = create_driver
    t = Time.now.to_i
    messages = [ARBITRARY_TEXT, JAVA_MYSQL_EXC, ARBITRARY_TEXT]
    d.run do
      feed_lines(d, t, *messages)
    end
    assert_equal(make_logs(t, *messages), d.events)
  end

  def test_ignore_nested_exceptions
    test_cases = {
      'php' => PHP_EXC,
      'python' => PYTHON_EXC,
      'ruby' => RUBY_EXC
    }

    test_cases.each do |language, exception|
      cfg = %(
#{CONFIG}
languages #{language})
      d = create_driver(cfg)
      t = Time.now.to_i

      # Convert exception to a single line to simplify the test case.
      single_line_exception = exception.gsub("\n", '\\n')

      # There is a nested exception within the body, we should ignore those!
      json_with_exception = {
        'timestamp' => {
          'nanos' => 998_152_494,
          'seconds' => 1_496_420_064
        },
        'message' => single_line_exception,
        'thread' => 139_658_267_147_048,
        'severity' => 'ERROR'
      }
      json_line_with_exception = "#{json_with_exception.to_json}\n"
      json_without_exception = {
        'timestamp' => {
          'nanos' => 5_990_266,
          'seconds' => 1_496_420_065
        },
        'message' => 'next line',
        'thread' => 139_658_267_147_048,
        'severity' => 'INFO'
      }
      json_line_without_exception = "#{json_without_exception.to_json}\n"

      router_mock = flexmock('router')

      # Validate that each line received is emitted separately as expected.
      router_mock.should_receive(:emit)
                 .once.with(DEFAULT_TAG_STRIPPED, Integer,
                            'message' => json_line_with_exception,
                            'count' => 0)

      router_mock.should_receive(:emit)
                 .once.with(DEFAULT_TAG_STRIPPED, Integer,
                            'message' => json_line_without_exception,
                            'count' => 1)

      d.instance.router = router_mock

      d.run do
        feed_lines(d, t, json_line_with_exception + json_line_without_exception)
      end
    end
  end

  def test_single_language_config
    cfg = %(
#{CONFIG}
languages java)
    d = create_driver(cfg)
    t = Time.now.to_i
    d.run do
      feed_lines(d, t, ARBITRARY_TEXT, JAVA_EXC, PYTHON_EXC)
    end
    expected = ARBITRARY_TEXT.lines + [JAVA_EXC] + PYTHON_EXC.lines
    assert_equal(make_logs(t, *expected), d.events)
  end

  def test_multi_language_config
    cfg = %(
#{CONFIG}
languages python, java)
    d = create_driver(cfg)
    t = Time.now.to_i
    d.run do
      feed_lines(d, t, ARBITRARY_TEXT, JAVA_EXC, PYTHON_EXC)
    end
    expected = ARBITRARY_TEXT.lines + [JAVA_EXC] + [PYTHON_EXC]
    assert_equal(make_logs(t, *expected), d.events)
  end

  def test_split_exception_after_timeout
    cfg = %(
#{CONFIG}
multiline_flush_interval 1)
    d = create_driver(cfg)
    t1 = 0
    t2 = 0
    d.run do
      t1 = Time.now.to_i
      feed_lines(d, t1, JAVA_EXC)
      sleep 2
      t2 = Time.now.to_i
      feed_lines(d, t2, "  at x\n  at y\n")
    end
    assert_equal(make_logs(t1, JAVA_EXC) +
                 make_logs(t2, "  at x\n", "  at y\n"),
                 d.events)
  end

  def test_do_not_split_exception_after_pause
    d = create_driver
    t1 = 0
    t2 = 0
    d.run do
      t1 = Time.now.to_i
      feed_lines(d, t1, JAVA_EXC)
      sleep 1
      t2 = Time.now.to_i
      feed_lines(d, t2, "  at x\n  at y\n")
      d.instance.before_shutdown
    end
    assert_equal(make_logs(t1, "#{JAVA_EXC}  at x\n  at y\n"), d.events)
  end

  def test_remove_tag_prefix_is_required
    cfg = ''
    e = assert_raises(Fluent::ConfigError) { create_driver(cfg) }
    assert_match(/remove_tag_prefix/, e.message)
  end

  def get_out_tags(remove_tag_prefix, original_tag)
    cfg = "remove_tag_prefix #{remove_tag_prefix}"
    d = create_driver(cfg, original_tag)
    run_driver(d, ARBITRARY_TEXT, JAVA_EXC, ARBITRARY_TEXT)
    d.emits.collect { |e| e[0] }.sort.uniq
  end

  def test_remove_tag_prefix
    tags = get_out_tags('prefix.plus', 'prefix.plus.rest.of.the.tag')
    assert_equal(['rest.of.the.tag'], tags)
    tags = get_out_tags('prefix.pl', 'prefix.plus.rest.of.the.tag')
    assert_equal(['prefix.plus.rest.of.the.tag'], tags)
    tags = get_out_tags('does.not.occur', 'prefix.plus.rest.of.the.tag')
    assert_equal(['prefix.plus.rest.of.the.tag'], tags)
  end

  def test_force_line_breaks_false
    cfg = %(
#{CONFIG}
force_line_breaks true)
    d = create_driver(cfg)
    t = Time.now.to_i
    d.run do
      feed_lines(d, t, JAVA_EXC)
    end
    expected = JAVA_EXC
    assert_equal(make_logs(t, *expected), d.events)
  end

  def test_force_line_breaks_true
    cfg = %(
#{CONFIG}
force_line_breaks true)
    d = create_driver(cfg)
    t = Time.now.to_i
    d.run do
      feed_lines_without_line_breaks(d, t, JAVA_EXC)
    end
    # Expected: the first two lines of the exception are buffered and combined.
    # Then the max_lines setting kicks in and the rest of the Python exception
    # is logged line-by-line (since it's not an exception stack in itself).
    # For the following Java stack trace, the two lines of the first exception
    # are buffered and combined. So are the first two lines of the second
    # exception. Then the rest is logged line-by-line.
    expected = JAVA_EXC.chomp
    assert_equal(make_logs(t, *expected), d.events)
  end

  def test_flush_after_max_lines
    cfg = %(
#{CONFIG}
max_lines 2)
    d = create_driver(cfg)
    t = Time.now.to_i
    d.run do
      feed_lines(d, t, PYTHON_EXC, JAVA_EXC)
    end
    # Expected: the first two lines of the exception are buffered and combined.
    # Then the max_lines setting kicks in and the rest of the Python exception
    # is logged line-by-line (since it's not an exception stack in itself).
    # For the following Java stack trace, the two lines of the first exception
    # are buffered and combined. So are the first two lines of the second
    # exception. Then the rest is logged line-by-line.
    expected = [PYTHON_EXC.lines[0..1].join] + PYTHON_EXC.lines[2..] + \
               [JAVA_EXC.lines[0..1].join] + [JAVA_EXC.lines[2..3].join] + \
               JAVA_EXC.lines[4..]
    assert_equal(make_logs(t, *expected), d.events)
  end

  def test_separate_streams
    cfg = %(
#{CONFIG}
stream stream)
    d = create_driver(cfg)
    t = Time.now.to_i
    d.run do
      feed_lines(d, t, JAVA_EXC.lines[0], stream: 'java')
      feed_lines(d, t, PYTHON_EXC.lines[0..1].join, stream: 'python')
      feed_lines(d, t, JAVA_EXC.lines[1..].join, stream: 'java')
      feed_lines(d, t, JAVA_EXC, stream: 'java')
      feed_lines(d, t, PYTHON_EXC.lines[2..].join, stream: 'python')
      feed_lines(d, t, 'something else', stream: 'java')
    end
    # Expected: the Python and the Java exceptions are handled separately
    # because they belong to different streams.
    # Note that the Java exception is only detected when 'something else'
    # is processed.
    expected = make_logs(t, JAVA_EXC, stream: 'java') +
               make_logs(t, PYTHON_EXC, stream: 'python') +
               make_logs(t, JAVA_EXC, stream: 'java') +
               make_logs(t, 'something else', stream: 'java')
    assert_equal(expected, d.events)
  end
end
