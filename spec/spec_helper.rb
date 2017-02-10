RSpec.shared_examples 'a plugin with invalid configuration' do
  describe '#stop' do
    let(:queue) { SizedQueue.new(20) }
    subject { described_class.new(config) }

    it 'fails to register' do
      begin
        subject.register
        fail 'ConfigurationError expected'
      rescue LogStash::ConfigurationError => ex
        # expected
      end
    end
  end
end