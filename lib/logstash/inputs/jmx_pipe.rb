# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require 'jdk_helper'
require 'jmx4r'

# A LogStash input plugin for connecting to JMX endpoints to periodically gather MBean attribute values and to subscribe
# to JMX notifications and logging those as events.
#
# This plugin was written after logstash-input-jmx was found to be, in my opinion, not only insufficient (it lacks
# notification subscription support) but also poorly designed. logstash-input-jmx_pipe does the following things differently:
# * It has no outside configuration; all configuration is done within the logstash.conf file.
# * It spawns no additional threads it would have to manage. Instead, user is required to configure multiple instances of the
#   plugin to monitor multiple JMX endpoints (or even if she wishes to distribute the load to multiple threads.)
# * It allows (and indeed requires) original MBean attribute names to be specified, not the snake_cased_ones.
# * It allows multiple MBeans to be queried to produce one LogStash event, or, alternatively, querying multiple MBeans using
#   wildcards and producing multiple events at once.
# * It allows logging values from the MBean object name key-value property pairs.
#
# === Configuration
# Let us start with an example of a configuration that makes use of all the functionality the plugin can offer.
#
# [source,ruby]
# ----------------------------------
#   jmx_pipe {
#     type => "jmx_data"
#     interval => 15000
#     host => "10.10.10.10"
#     port => 3000
#     username => "monitorRole"
#     password => "password"
#     event_context => {
#       server_name => "catalog-service"
#     }
#     queries => [
#       {
#         name => "Heap"
#         objects => {
#           "java.lang:type=Memory" => {
#             "HeapMemoryUsage" => "Usage"
#           }
#         }
#       },
#       {
#         name => "Load"
#         objects => {
#           "java.lang:type=OperatingSystem" => {
#             "SystemCpuLoad" => "SystemCpuLoad"
#             "SystemLoadAverage" => "SystemLoadAverage"
#             "ProcessCpuLoad" => "ProcessCpuLoad"
#             "AvailableProcessors" => "AvailableProcessors"
#           }
#         }
#       }
#     ]
#     subscriptions => [
#       {
#         name => "GCEvent"
#         object => "java.lang:type=GarbageCollector,name=*"
#         attributes => {
#           "gcAction" => "GCAction"
#           "=name" => "GCName"
#           "gcCause" => "GCCause"
#           "gcInfo.duration" => "GCDuration"
#         }
#       }
#     ]
#   }
# ----------------------------------
#
# A brief explanation of the configuration keys follows.
#
# * `type` may be used to modify the `_type` property of the events.
# * `interval` is a polling interval in milliseconds for querying the MBean attribute values. Note that this interval does
#   not include the time it takes to do the querying itself. If the querying ever takes a longer time than the given interval,
#   the next iteration of querying will be done right after the one that took too long. That will also be the case should the
#   querying take more than two times the length of the interval, however, effort will not be made to make up for all the
#   missed iterations; all but the last one will be skipped. Such events are logged to the logstash log with a "warn"
#   severity.
# * `host` and `port` describe the target JMX endpoint. Only one endpoint can be observed using one instance of the plugin;
#   configure more than one in the logstash.conf to query and/or subscribe to multiple JMX endpoints.
# * `username` and `password` are optional. Required in case the JMX endpoint mandates authentication.
# * `event_context` is an optional hash of properties to be appended to each emitted event.
#
# Those were the configuration keys to get the plugin ready for work, which is querying MBeans and subscribing to JMX
# notifications. How to configure those operations is described in the following two subsections.
#
# ==== The `queries` configuration key
#
# For each hash in the (optional) `queries` list, the plugin will, in each iteration, query all the listed `objects` (keys of
# the `objects` hash represent object name patterns). If any object is found, there are three possible situations:
# * One or more object name patterns are listed in the `objects` hash and at most one of each is found. Then, a single event
#   is emitted with the property `name` set to the value of the query's `name` configuration key. Other properties of the
#   event contain values of the MBeans' attributes as the values of the `objects` has specify. Each entry in the hash for
#   each object name pattern contains a mapping from MBean attribute name (as the key) to the LogStash event property name
#   (as the value). `"ResourcesInUse" => "WebWorkerThreadsIsUse"` thus means the LogStash event will have a property named
#   `WebWorkerThreadsInUse` that will carry the value of the `ResourceInUse` attribute of the found MBean.
# * One object name pattern containing wildcard(s) is listed in the `objects` hash and multiple MBeans have been found. Then
#   for each found object an event is emitted, carrying its attributes' values as properties as configured.
# * Multiple object name patterns are listed in the `objects` hash, at least one containing a wildcard, and multiple
#   MBeans have been found for at least one of the patterns. In this case, a warning is logged in the logstash log that only
#   the first of those MBeans will be taken into account. Then, the plugin proceeds in the same way as if only one (ie.
#   first) MBean had been found for each pattern.
#
# The MBean attribute name specified in the mapping can also be a simple expression to __climb down__ a composite value tree.
# Properties of the composite value are accessed using a dot, i.e. `attribute.property.subproperty`. See the last paragraph
# of the next subsection (about `subscriptions`) to see an example.
#
# ==== The `subscriptions` configuration key
#
# This plugin can subscribe to notifications that are emitted by the MBeans. At the moment, no filtering is supported.
# Filtering can be done later in the LogStash pipeline, although that is hardly a good solution: there is a good reason the
# JMX notification subscription mechanism natively supports filtering.
#
# `subscriptions` is an optional list of hashes with the following attributes.
# * `name` is a string that is set as a value of the `name` property on each emitted LogStash event.
# * `object` is an object name pattern for locating the object to which a subscription is to be made. Note that if multiple
#   objects are found, a subscription is made to all of them!
# * `attributes` is a hash that describes a mapping of notification attributes' names (the keys) to event property names
#   (the values) in a similar fashion as the outermost hashes in the `objects` hash describe a mapping of the MBean attribute
#   names.
#
# The notification attribute name specified in the mapping can also be a simple expression to __climb down__ a composite
# value tree. Properties of the composite value are accessed using a dot, i.e. `attribute.property.subproperty`. In the
# example, `gcInfo` is a composite value that, among others, contains a primitive value named `duration`.
#
# Note that we could also map the entire `gcInfo` without specifying one of its keys and the plugin will unpack the
# `gcInfo`'s values to properties with underscores in names. We would advise against using this functionality.
#
# ==== Using a secure (SSL) JMX connection
#
# If the JMX endpoint requires a secure connection (which it should), the truststore with the server's public key must be
# presented to the JVM in which LogStash runs. LogStash should be run with the following parameter in the `JAVA_OPTS`
# environment variable: `-Djavax.net.ssl.trustStore=/etc/logstash/jmx.truststore`, where the given file is the truststore
# with all the needed public keys.


class LogStash::Inputs::JmxPipe < LogStash::Inputs::Base
  java_import javax.management.NotificationListener
  java_import javax.management.ObjectName

  config_name 'jmx_pipe'
  milestone 1

  #TODO loggers should state the host+port OR some other identifier

  config :host, :validate => :string, :required => true
  config :port, :validate => :number, :required => true
  config :username, :validate => :string, :required => false
  config :password, :validate => :string, :required => false
  config :interval, :validate => :number, :required => true
  config :event_context, :validate => :hash, :required => false
  config :queries, :validate => :array, :required => false
  config :subscriptions, :validate => :array, :required => false

  public
  def register
    @queries ||= []
    @subscriptions ||= []

    validation_result = validate_queries(@queries) || validate_subscriptions(@subscriptions)
    unless validation_result.nil?
      @logger.error validation_result
      raise LogStash::ConfigurationError::new validation_result
    end

    @stop_event = Concurrent::Event::new
    @subscriptions_to_add = @subscriptions.clone

    require 'jmx4r'
    @next_iteration = Time::now + @interval
  end

  private
  def validate_queries(queries)
    queries.each_with_index do |query, i|
      unless query.respond_to?(:has_key?) and query.respond_to?(:each)
        return "queries[#{i}] is not an object"
      end
      unless query.has_key?('name') and not query['name'].nil? and query['name'].instance_of? String
        return "queries[#{i}].name missing or not a string"
      end
      unless query.has_key?('objects') and not query['objects'].nil? and query['objects'].respond_to?(:has_key?) and
          query['objects'].respond_to?(:each) and query['objects'].respond_to?(:each_entry)
        return "queries[#{i}].objects missing or not an object"
      end
      query['objects'].each_entry do |obj_name, attr_spec|
        unless not obj_name.nil? and obj_name.instance_of? String
          return "One of the queries[#{i}].objects key is not a string"
        end
        unless not attr_spec.nil? and attr_spec.respond_to?(:has_key?) and attr_spec.respond_to?(:each) and
            attr_spec.respond_to?(:each_entry)
          return "One of the queries[#{i}].objects key is not an object"
        end
        attr_spec.each_entry do |attr_path, attr_alias|
          unless not attr_path.nil? and attr_path.instance_of? String
            return "One of the queries[#{i}].objects[#{attr_path}] keys is not a string"
          end
          unless not attr_alias.nil? and attr_alias.instance_of? String
            return "One of the queries[#{i}].objects[#{attr_path}] values is not a string"
          end
        end
      end
    end
    nil
  end

  private
  def validate_subscriptions(subscriptions)
    subscriptions.each_with_index do |subscription, i|
      unless subscription.respond_to?(:has_key?) and subscription.respond_to?(:each)
        return "subscriptions[#{i}] is not an object"
      end
      unless subscription.has_key?('name') and not subscription['name'].nil? and subscription['name'].instance_of? String
        return "subscriptions[#{i}].name missing or not a string"
      end
      unless subscription.has_key?('object') and not subscription['object'].nil? and subscription['object'].instance_of? String
        return "subscriptions[#{i}].object missing or not a string"
      end
      unless subscription.has_key?('attributes') and not subscription['attributes'].nil? and subscription['attributes'].respond_to?(:has_key?) and
          subscription['attributes'].respond_to?(:each) and subscription['attributes'].respond_to?(:each_entry)
        return "subscriptions[#{i}].attributes missing or not an object"
      end
      subscription['attributes'].each_entry do |attr_path, attr_alias|
        unless not attr_path.nil? and attr_path.instance_of? String
          return "One of the subscriptions[#{i}].attributes[#{attr_path}] keys is not a string"
        end
        unless not attr_alias.nil? and attr_alias.instance_of? String
          return "One of the subscriptions[#{i}].attributes[#{attr_path}] values is not a string"
        end
      end
    end
    nil
  end

  public
  def stop
    @stop_event.set unless @stop_event.nil?
  end

  public
  def teardown
    @logger.info 'Shutting down.'
    @interrupt
    finished
  end

  public
  def run(queue)
    begin
      @queue = queue

      jmx_connection = nil
      event_context = @event_context || {}
      until @stop_called.true?
        begin
          jmx_connection ||= make_connection

          resubscribe_to_notifications jmx_connection

          @queries.each do |query|
            any_commit_done = FALSE
            query['objects'].each_entry do |bean_name, attr_spec|
              values = event_context.clone
              jmx_objects = JMX::MBean.find_all_by_name bean_name, :connection => jmx_connection
              if jmx_objects.length > 0
                if query['objects'].length > 1 and jmx_objects.length > 1
                  @logger.warn "Found #{jmx_objects.length} object(s) for #{bean_name}; avoiding combinatorial explosion by only querying the 1st object!"
                  jmx_objects = [jmx_objects[0]]
                else
                  @logger.debug "Found #{jmx_objects.length} object(s) for #{bean_name}"
                end
                jmx_objects.each do |jmx_object|
                  begin
                    query(jmx_connection, jmx_object, attr_spec, values)
                  rescue LogStash::ShutdownSignal => e
                    raise e
                  rescue Java::JavaRmi::ConnectException => e
                    raise e
                  rescue Exception => e
                    @logger.error 'Unable to process one of the JMX objects: ' + e.message + "\n " + e.backtrace.join("\n ")
                  end
                  if query['objects'].length == 1
                    # If we query only one object, it might be a wildcard query, so commit each one separately
                    send_event_to_queue(query['name'], values)
                    values = event_context.clone
                    any_commit_done = TRUE
                  end
                end
              else
                @logger.warn "No jmx object found for #{bean_name}"
              end
            end
            unless any_commit_done or values.empty?
              # This happens when we query by more than one object, or when there were no objects found
              send_event_to_queue(query['name'], values)
            end
          end
          sleep_until_next_iteration
        rescue LogStash::ShutdownSignal
          break
        rescue Java::JavaRmi::ConnectException => e
          @logger.error 'Connection lost; will try to reestablish in a second. Error was: ' + e.message + "\n " + e.backtrace.join("\n ")
          jmx_connection = nil
          @subscriptions_to_add = @subscriptions.clone
          sleep(1)
        rescue Exception => e
          @logger.error e.message + "\n " + e.backtrace.join("\n ")
          sleep_until_next_iteration
        end
      end
    rescue LogStash::ShutdownSignal
      # ignored
    rescue Exception => e
      @logger.error 'Cannot setup jmx_pipe, aborting:' + e.message + "\n " + e.backtrace.join("\n ")
    end
  end

  private
  def make_connection
    credentials = @username.nil? || @username.length == 0 ? nil : [@username, @password].to_java(:String)

    @logger.info "Establishing a connection to #{@host}: #{@port}"
    JMX::MBean.create_connection :host => @host, :port => @port, :credentials => credentials
  end

  private
  def resubscribe_to_notifications(jmx_connection)
    @subscriptions_to_add.delete_if do |subscription|
      begin
        object_namespec = subscription['object']
        subscription_name = subscription['name']
        subscription_attributes = subscription['attributes']

        jmx_objects = JMX::MBean.find_all_by_name object_namespec, :connection => jmx_connection
        if jmx_objects.length == 0
          @logger.info "No bean found for name #{object_namespec}; postponing notification subscription."
          false
        end

        jmx_objects.each do |mbean|
          @logger.debug "Successfully added notification listener to bean #{mbean.object_name}"
          jmx_connection.addNotificationListener(
              mbean.object_name,
              JmxPipeNotificationListener.new(self, subscription_name, subscription_attributes),
              nil,
              nil)
        end
        true
      rescue Exception => e
        @logger.warn "Error while setting up a notification listener #{subscription_name}: " +
                         e.message + "\n " + e.backtrace.join("\n ")
        false
      end
    end
  end

  private
  def sleep_until_next_iteration
    sleep_time = @next_iteration - Time::now
    if sleep_time < 0
      skip_iterations = (-sleep_time / @interval).to_i
      @logger.warn ("Overshot the planned iteration time for #{(-sleep_time).to_s} seconds" +
          (skip_iterations > 0 ? ', skipping ' + skip_iterations.to_s + ' iterations' : '') +
          ', querying immediately!')
      @next_iteration += skip_iterations * @interval
    end

    if sleep_time > 0
      @logger.debug "Sleeping for #{sleep_time.to_s} seconds."
      @stop_event.wait(sleep_time)
    end
    @next_iteration += @interval
  end

  private
  def query(jmx_connection, jmx_object, attr_spec, result_values)
    attr_names = attr_spec.select{|attr_path, _| not attr_path.start_with?('=')}.keys.map{|attr_path| attr_path.split('.')[0]}
    attr_values = attr_names.empty? ? [] : values = jmx_connection.getAttributes(jmx_object.object_name, attr_names)
    attr_values_hash = Hash[attr_values.collect{|a| [a.getName, a.getValue]}]

    attr_spec.each_entry do |attr_path, attr_alias|
      attr_path_parts = attr_path.split('.')
      attr_name = attr_path_parts[0]

      if attr_name.start_with?('=')
        value = jmx_object.object_name.getKeyProperty(attr_name[1..-1])
        if value.start_with?('"')
          value = javax.management.ObjectName.unquote(value)
        end
      else
        value = attr_values_hash[attr_name]
      end

      convert_and_set(value, attr_alias, result_values, attr_path_parts[1..-1])
    end
  end

  public
  def handle_notification(notification, event_name, attributes)
    begin
      values = @event_context.nil? ? {} : @event_context.clone
      values[:message] = notification.getMessage

      attributes.each_entry do |attr_path, attr_alias|
        attr_path_parts = attr_path.split('.')
        convert_and_set(notification.getUserData, attr_alias, values, attr_path_parts)
      end

      send_event_to_queue(event_name, values)
    rescue Exception => e
      @logger.warn "Error handling notification #{event_name}: " + e.message + "\n " + e.backtrace.join("\n ")
    end
  end

  class JmxPipeNotificationListener
    java_implements javax.management.NotificationListener

    def initialize(jmx_pipe, event_name, attributes)
      @jmx_pipe = jmx_pipe
      @event_name = event_name
      @attributes = attributes
    end

    java_signature 'void handleNotification(javax.management.Notification, java.lang.Object)'
    def handle_notification(notification, context)
      @jmx_pipe.logger.debug "Handling notification #{@event_name}."
      @jmx_pipe.handle_notification(notification, @event_name, @attributes)
    end
  end

  private
  def convert_and_set(value, attr_alias, result_values, path = [])
    if not value.nil? and value.instance_of? Java::JavaxManagementOpenmbean::CompositeDataSupport
      if path.nil? or path.empty?
        value.each do |subattr_name|
          convert_and_set(value[subattr_name], attr_alias + '_' + subattr_name, result_values, path[1..-1])
        end
      else
        subattr_name = path[0]
        if value.contains_key?(subattr_name)
          convert_and_set(value[subattr_name], attr_alias, result_values, path[1..-1])
        else
          @logger.warn "Parsing attribute #{attr_alias} failed: no field named #{subattr_name}"
        end
      end
    else
      #TODO Notification userData may be any sort of an object; maybe we should also check whether value is either a map or responds to the first path segment with a subvalue
      if path.nil? or path.empty?
        if value.nil?
          return
        end

        number_type = [Fixnum, Bignum, Float]
        boolean_type = [TrueClass, FalseClass]

        if boolean_type.include?(value.class) then
          value = value ? 1 : 0
        end

        result_values[attr_alias] = number_type.include?(value.class) ? value : value.to_s
      else
        @logger.warn "Parsing attribute #{attr_alias} failed: non-composite value observed when trying to traverse " +
                         "the leftover path: #{path.join('.')}"
      end
    end
  end

  private
  def send_event_to_queue(name, values)
    @logger.debug("Sending event #{name} to the logstash queue.")
    event = LogStash::Event.new
    event.set('host', @host)
    event.set('name', name)

    values.each do |key, value|
      event.set(key.to_s, value) unless value.nil?
    end

    decorate(event)
    @queue << event
  end
end