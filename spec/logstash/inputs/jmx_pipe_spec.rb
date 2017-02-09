# encoding: utf-8
require 'forwardable'
require 'spec_helper'
require 'stud/temporary'
require 'jmx4r'

describe LogStash::Inputs::JmxPipe do
  minimal_configuration = {
      'host' => 'localhost',
      'port' => 1050,
      'polling_frequency' => 15
  }


  context 'configuration validation' do
    it 'checks for nil host' do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.except('host')).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it 'checks for nil port' do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.except('port')).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it 'checks for nil polling_frequency' do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.except('polling_frequency')).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it 'checks for non-number port' do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.merge({'port' => {'test': 'test'}})).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it 'checks for non-number polling_frequency' do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.merge({'polling_frequency' => {'test': 'test'}})).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it 'allows nil queries and/or subscriptions' do
      LogStash::Inputs::JmxPipe::new(@params = minimal_configuration).register
    end

    it 'enforces queries to be a list' do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.merge({'queries' => {'test': 'test'}})).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it 'enforces queries to contain hashes' do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.merge({'queries' => ['test']})).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it 'enforces queries to specify name' do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.merge({'queries' => [{'objects' => {}}]})).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it 'enforces queries to specify objects' do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.merge({'queries' => [{'name' => 'Test'}]})).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it "enforces queries' objects to be a hash" do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.merge({'queries' => [{'name' => 'Test', 'objects' => []}]})).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it "enforces queries' objects to be a string->* hash" do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.merge({'queries' => [{'name' => 'Test', 'objects' => {9 => {}}}]})).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it "enforces queries' objects to be a *->hash hash" do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.merge({'queries' => [{'name' => 'Test', 'objects' => {'test' => 'test'}}]})).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it "enforces queries' objects' values to be a string->* hash" do
      begin
        LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.merge({'queries' => [{'name' => 'Test', 'objects' => {'test' => {9 => 'test'}}}]})).register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end

    it 'confirms a proper queries setting' do
      LogStash::Inputs::JmxPipe::new(@params = minimal_configuration.merge({'queries' => [{'name' => 'Test', 'objects' => {'test' => {'test' => 'test'}}}]})).register
    end

    #TODO Check for 'subscriptions' validation
  end

  context 'connection (re)establishment' do
    #TODO
  end

  context 'querying' do
    #TODO
  end

  context 'notification subscription (re)instantiation and handling' do
    #TODO
  end
end
