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
    ### Error updating database.  Cause: com.mysql.jdbc.MysqlDataTruncation: Data truncation: Data too long for column 'LOG_OPERATE_PARAMS' at row 1			
    ### The error may involve com.haier.cosmosom.persistence.system.dao.ISystemLogDAO.insert-Inline			
    ### The error occurred while setting parameters			
    ### SQL: INSERT INTO     system_log_2024_03    (   ID ,   LOG_TYPE ,   LOG_TITLE ,   LOG_REMOTE_ADDR ,   LOG_USER_AGENT ,   LOG_REQUEST_URI ,   LOG_OPERATE_METHOD_NAME ,   LOG_OPERATE_METHOD ,   LOG_OPERATE_PARAMS ,   LOG_OPERATE_EXCEPTION ,   PROJECT_CODE ,   RESOURCE_ID ,   DATA_SCOPE ,   CREATE_BY_NAME ,   CREATE_BY ,   CREATE_DATE ,   REMARKS ,   BAK1 ,   BAK2 ,   BAK3   ) VALUES (   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?  ,   ?   )			
    ### Cause: com.mysql.jdbc.MysqlDataTruncation: Data truncation: Data too long for column 'LOG_OPERATE_PARAMS' at row 1			
    ; SQL []; Data truncation: Data too long for column 'LOG_OPERATE_PARAMS' at row 1; nested exception is com.mysql.jdbc.MysqlDataTruncation: Data truncation: Data too long for column 'LOG_OPERATE_PARAMS' at row 1			
      at org.springframework.jdbc.support.SQLStateSQLExceptionTranslator.doTranslate(SQLStateSQLExceptionTranslator.java:102)			
      at org.springframework.jdbc.support.AbstractFallbackSQLExceptionTranslator.translate(AbstractFallbackSQLExceptionTranslator.java:73)			
      at org.springframework.jdbc.support.AbstractFallbackSQLExceptionTranslator.translate(AbstractFallbackSQLExceptionTranslator.java:81)			
      at org.springframework.jdbc.support.AbstractFallbackSQLExceptionTranslator.translate(AbstractFallbackSQLExceptionTranslator.java:81)			
      at org.mybatis.spring.MyBatisExceptionTranslator.translateExceptionIfPossible(MyBatisExceptionTranslator.java:75)			
      at org.apache.ibatis.binding.MapperProxy.invoke(MapperProxy.java:53)			
      at com.sun.proxy.$Proxy128.insert(Unknown Source)			
      at net.siufung.boot.mybatis.support.ServiceImpl.insert(ServiceImpl.java:39)			
      at net.siufung.boot.mybatis.support.ServiceImpl$$FastClassBySpringCGLIB$$4f8a5337.invoke(<generated>)			
      at org.springframework.cglib.proxy.MethodProxy.invoke(MethodProxy.java:204)			
      at org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.invokeJoinpoint(CglibAopProxy.java:721)			
      at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:157)			
      at org.springframework.transaction.interceptor.TransactionInterceptor$1.proceedWithInvocation(TransactionInterceptor.java:99)			
      at org.springframework.transaction.interceptor.TransactionAspectSupport.invokeWithinTransaction(TransactionAspectSupport.java:282)			
      at org.springframework.transaction.interceptor.TransactionInterceptor.invoke(TransactionInterceptor.java:96)			
      at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:179)			
      at org.springframework.aop.framework.CglibAopProxy$DynamicAdvisedInterceptor.intercept(CglibAopProxy.java:656)			
      at com.haier.cosmosom.persistence.system.service.SystemLogService$$EnhancerBySpringCGLIB$$c5183cb.insert(<generated>)			
      at com.haier.cosmosom.leaflog.aspect.LeafLogAspect.lambda$handlerLeafLog$0(LeafLogAspect.java:258)			
      at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)			
      at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)			
      at java.lang.Thread.run(Thread.java:750)			
    Caused by: com.mysql.jdbc.MysqlDataTruncation: Data truncation: Data too long for column 'LOG_OPERATE_PARAMS' at row 1			
      at com.mysql.jdbc.MysqlIO.checkErrorPacket(MysqlIO.java:3971)			
      at com.mysql.jdbc.MysqlIO.checkErrorPacket(MysqlIO.java:3909)			
      at com.mysql.jdbc.MysqlIO.sendCommand(MysqlIO.java:2527)			
      at com.mysql.jdbc.MysqlIO.sqlQueryDirect(MysqlIO.java:2680)			
      at com.mysql.jdbc.ConnectionImpl.execSQL(ConnectionImpl.java:2494)			
      at com.mysql.jdbc.PreparedStatement.executeInternal(PreparedStatement.java:1858)			
      at com.mysql.jdbc.PreparedStatement.execute(PreparedStatement.java:1197)			
      at com.alibaba.druid.filter.FilterChainImpl.preparedStatement_execute(FilterChainImpl.java:2931)			
      at com.alibaba.druid.filter.FilterEventAdapter.preparedStatement_execute(FilterEventAdapter.java:440)			
      at com.alibaba.druid.filter.FilterChainImpl.preparedStatement_execute(FilterChainImpl.java:2929)			
      at com.alibaba.druid.filter.FilterEventAdapter.preparedStatement_execute(FilterEventAdapter.java:440)			
      at com.alibaba.druid.filter.FilterChainImpl.preparedStatement_execute(FilterChainImpl.java:2929)			
      at com.alibaba.druid.proxy.jdbc.PreparedStatementProxyImpl.execute(PreparedStatementProxyImpl.java:131)			
      at com.alibaba.druid.pool.DruidPooledPreparedStatement.execute(DruidPooledPreparedStatement.java:493)			
      at org.apache.ibatis.executor.statement.PreparedStatementHandler.update(PreparedStatementHandler.java:46)			
      at org.apache.ibatis.executor.statement.RoutingStatementHandler.update(RoutingStatementHandler.java:74)			
      at sun.reflect.GeneratedMethodAccessor385.invoke(Unknown Source)			
      at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)			
      at java.lang.reflect.Method.invoke(Method.java:498)			
      at org.apache.ibatis.plugin.Plugin.invoke(Plugin.java:63)			
      at com.sun.proxy.$Proxy416.update(Unknown Source)			
      at sun.reflect.GeneratedMethodAccessor385.invoke(Unknown Source)			
      at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)			
      at java.lang.reflect.Method.invoke(Method.java:498)			
      at org.apache.ibatis.plugin.Plugin.invoke(Plugin.java:63)			
      at org.apache.ibatis.executor.SimpleExecutor.doUpdate(SimpleExecutor.java:50)			
      at org.apache.ibatis.executor.BaseExecutor.update(BaseExecutor.java:117)			
      at org.apache.ibatis.executor.CachingExecutor.update(CachingExecutor.java:76)			
      at org.apache.ibatis.session.defaults.DefaultSqlSession.update(DefaultSqlSession.java:198)			
      at org.apache.ibatis.session.defaults.DefaultSqlSession.insert(DefaultSqlSession.java:185)			
      at sun.reflect.GeneratedMethodAccessor447.invoke(Unknown Source)			
      at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)			
      at java.lang.reflect.Method.invoke(Method.java:498)			
      at org.mybatis.spring.SqlSessionTemplate$SqlSessionInterceptor.invoke(SqlSessionTemplate.java:434)
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
    messages = [ARBITRARY_TEXT, JAVA_EXC, ARBITRARY_TEXT]
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
