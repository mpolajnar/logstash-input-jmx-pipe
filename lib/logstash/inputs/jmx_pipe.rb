# encoding: utf-8
require "logstash/inputs/jmx_pipe/version"

require "logstash/inputs/base"
require "logstash/namespace"

require 'jdk_helper'

class LogStash::Inputs::JmxPipe < LogStash::Inputs::Base
  java_import javax.management.NotificationListener

  config_name 'jmx_pipe'
  milestone 1

  #TODO loggers should state the host+port OR some other identifier

  config :host, :validate => :string, :required => true
  config :port, :validate => :number, :required => true
  config :username, :validate => :string, :required => false
  config :password, :validate => :string, :required => false
  config :polling_frequency, :validate => :number, :required => true
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
    @next_iteration = Time::now + polling_frequency
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
                if jmx_objects.length > 0
                  if query['objects'].length > 1
                    @logger.warn "Found #{jmx_objects.length} object(s) for #{bean_name}; avoiding combinatorial explosion by only querying the 1st object!"
                    jmx_objects = [jmx_objects[0]]
                  else
                    @logger.debug "Found #{jmx_objects.length} object(s) for #{bean_name}"
                  end
                end
                jmx_objects.each do |jmx_object|
                  query(jmx_connection, jmx_object, attr_spec, values)
                end
                if query['objects'].length == 1
                  # If we query only one object, it might be a wildcard query, so commit each one separately
                  send_event_to_queue(query['name'], values)
                  values = event_context.clone
                  any_commit_done = TRUE
                end
              else
                @logger.warn "No jmx object found for #{bean_name}"
              end
            end
            unless any_commit_done
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
      skip_iterations = (-sleep_time / @polling_frequency).to_i
      @logger.warn ("Overshot the planned iteration time for #{(-sleep_time).to_s} seconds" +
          (skip_iterations > 0 ? ', skipping ' + skip_iterations.to_s + ' iterations' : '') +
          ', querying immediately!')
      @next_iteration += skip_iterations * @polling_frequency
    end

    if sleep_time > 0
      @logger.debug "Sleeping for #{sleep_time.to_s} seconds."
      @stop_event.wait(sleep_time)
    end
    @next_iteration += @polling_frequency
  end

  private
  def query(jmx_connection, jmx_object, attr_spec, result_values)
    attr_spec.each_entry do |attr_path, attr_alias|
      attr_path_parts = attr_path.split('.')
      attr_name = attr_path_parts[0]
      value = jmx_connection.getAttribute(jmx_object.object_name, attr_name)

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