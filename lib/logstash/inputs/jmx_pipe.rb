# encoding: utf-8
require "logstash/inputs/jmx_pipe/version"

require "logstash/inputs/base"
require "logstash/namespace"

class LogStash::Inputs::JmxPipe < LogStash::Inputs::Base
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
    @stop_event = Concurrent::Event::new
    #TODO Additional configuration validation

    require 'jmx4r'
    @next_iteration = Time::now + polling_frequency
  end

  public
  def run(queue)
    begin
      #TODO If at startup the JMX process is unavailable or host unresolvable, we should probably still allow start
      #TODO If the JMX process disappears, the connection might need to be reestablished; try that out
      jmx_connection = make_connection

      event_context = @event_context || {}
      until @stop_called.true?
        begin
          queries.each do |query|
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
                  send_event_to_queue(queue, query['name'], values)
                  values = event_context.clone
                  any_commit_done = TRUE
                end
              else
                @logger.warn "No jmx object found for #{bean_name}"
              end
            end
            unless any_commit_done
              # This happens when we query by more than one object, or when there were no objects found
              send_event_to_queue(queue, @host, query['name'], values)
            end
          end

          sleep_until_next_iteration
        rescue LogStash::ShutdownSignal
          break
        rescue Exception => e
          @logger.error e.message + "\n " + e.backtrace.join("\n ")
        end
      end
    rescue LogStash::ShutdownSignal
      # ignored
    rescue Exception => e
      @logger.error e.message + "\n " + e.backtrace.join("\n ")
    end
  end

  private
  def make_connection
    credentials = @username.nil? || @username.length == 0 ? nil : [@username, @password].to_java(:String)

    @logger.info "Establishing a connection to #{@host}: #{@port}"
    JMX::MBean.create_connection :host => @host, :port => @port, :credentials => credentials
  end

  private
  def sleep_until_next_iteration
    sleep_time = @next_iteration - Time::now
    if sleep_time < 0
      skip_iterations = (-sleep_time / @polling_frequency).to_i
      @logger.warn ("Overshot the planned iteration time for #{(-sleep_time).to_s} seconds" +
          (skip_iterations > 0 ? ', skipping ' + skip_iterations.to_s + ' iterations' : '') +
          '!')
      @next_iteration += skip_iterations * @polling_frequency
    end

    @logger.debug "Sleeping for #{sleep_time.to_s} seconds."
    @stop_event.wait(sleep_time) unless sleep_time < 0
    @next_iteration += @polling_frequency
  end

  public
  def stop
    @stop_event.set
  end

  private
  def query(jmx_connection, jmx_object, attr_spec, result_values)
    attr_spec.each_entry do |attr_name, attr_alias|
      value = jmx_connection.getAttribute(jmx_object.object_name, attr_name)

      if not value.nil? and value.instance_of? Java::JavaxManagementOpenmbean::CompositeDataSupport
        value.each do |subvalue|
          result_values[attr_alias + '_' + subvalue.to_s] = convert_single_value(value[subvalue])
        end
      else
        result_values[attr_alias] = convert_single_value(value)
      end
    end
  end

  private
  def convert_single_value(value)
    if value.nil?
      return nil
    end

    number_type = [Fixnum, Bignum, Float]
    boolean_type = [TrueClass, FalseClass]

    if boolean_type.include?(value.class) then
      value = value ? 1 : 0
    end

    return number_type.include?(value.class) ? value : value.to_s
  end

  private
  def send_event_to_queue(queue, name, values)
    @logger.debug('Send event to queue to be processed by filters/outputs')
    event = LogStash::Event.new
    event.set('host', @host)
    event.set('name', name)

    values.each do |key, value|
      event.set(key, value) unless value.nil?
    end

    decorate(event)
    queue << event
  end

  public
  def teardown
    @logger.debug 'Shutting down.'
    @interrupt
    finished
  end
end