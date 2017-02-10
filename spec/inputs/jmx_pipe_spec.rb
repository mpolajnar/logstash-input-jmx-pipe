# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "spec_helper.rb"
require "logstash/inputs/jmx_pipe"

describe LogStash::Inputs::JmxPipe do

  minimal_configuration = {
      'host' => 'localhost',
      'port' => 1050,
      'interval' => 15000
  }

  good_queries = {'queries' => [{'name' => 'Test', 'objects' => {'test' => {'test' => 'test'}}}]}
  good_subscriptions = {'subscriptions' => [{'name' => 'Test', 'object' => 'obj', 'attributes' => {'test' => 'test'}}]}

  context 'with proper configuration' do
    it_behaves_like 'an interruptible input plugin' do
      let(:config) { minimal_configuration.merge({ "interval" => 100 }) }
    end

    it_behaves_like 'an interruptible input plugin' do
      let(:config) { minimal_configuration.merge(good_queries) }
    end

    it_behaves_like 'an interruptible input plugin' do
      let(:config) { minimal_configuration.merge(good_subscriptions) }
    end

    it_behaves_like 'an interruptible input plugin' do
      let(:config) { minimal_configuration.merge(good_subscriptions).merge(good_queries) }
    end
  end

  context 'with nil host' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.except('host') }
    end
  end

  context 'with nil port' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.except('port') }
    end
  end

  context 'with nil interval' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.except('interval') }
    end
  end

  context 'with non-numeric port' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'port' => {'test': 'test'}}) }
    end
  end

  context 'with non-numeric interval' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'interval' => {'test': 'test'}}) }
    end
  end

  context 'with non-list "queries" list' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'queries' => {'test': 'test'}}) }
    end
  end

  context 'with non-hash queries' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'queries' => ['test']}) }
    end
  end

  context 'with queries without a name' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'queries' => [{'objects' => {}}]}) }
    end
  end

  context 'with queries without objects' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'queries' => [{'name' => 'Test'}]}) }
    end
  end

  context 'with queries with wrongly typed "objects"' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'queries' => [{'name' => 'Test', 'objects' => []}]}) }
    end

    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'queries' => [{'name' => 'Test', 'objects' => []}]}) }
    end

    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'queries' => [{'name' => 'Test', 'objects' => {9 => {}}}]}) }
    end

    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'queries' => [{'name' => 'Test', 'objects' => {'test' => 'test'}}]}) }
    end

    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'queries' => [{'name' => 'Test', 'objects' => {'test' => 'test'}}]}) }
    end

    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'queries' => [{'name' => 'Test', 'objects' => {'test' => {9 => 'test'}}}]}) }
    end
  end

  context 'with non-list "subscriptions" list' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'subscriptions' => {'test': 'test'}}) }
    end
  end

  context 'with non-hash subscriptions' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'subscriptions' => ['test']}) }
    end
  end

  context 'with subscriptions without a name' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'subscriptions' => [{'object' => 'obj', 'attributes' => {}}]}) }
    end
  end

  context 'with subscriptions without an object name' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'subscriptions' => [{'name' => 'Test', 'attributes' => {}}]}) }
    end
  end

  context 'with subscriptions without attributes' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'subscriptions' => [{'name' => 'Test', 'object' => 'obj'}]}) }
    end
  end

  context 'with subscriptions with wrongly typed "attributes"' do
    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'subscriptions' => [{'name' => 'Test', 'object' => 'obj', 'attributes' => 9}]}) }
    end

    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'subscriptions' => [{'name' => 'Test', 'object' => 'obj', 'attributes' => {9 => 'test'}}]}) }
    end

    it_behaves_like 'a plugin with invalid configuration' do
      let(:config) { minimal_configuration.merge({'subscriptions' => [{'name' => 'Test', 'object' => 'obj', 'attributes' => {'test' => 9}}]}) }
    end
  end

  #TODO test connection (re)establishment
  #TODO test querying
  #TODO test notification subscription (re)instantiation and handling
end
