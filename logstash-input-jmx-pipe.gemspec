# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'logstash/inputs/jmx_pipe/version'

Gem::Specification.new do |spec|
  spec.name          = 'logstash-input-jmx-pipe'
  spec.version       = Logstash::Inputs::JmxPipe::VERSION
  spec.authors       = ['Matija Polajnar, Marand d.o.o.']
  spec.email         = ['matija.polajnar@gmail.com', 'info@marand.si']

  spec.summary       = 'Retrieve metrics subscribe to notifications from a single JMX source.'
  spec.description   = 'This gem is a Logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/logstash-plugin install gemname. This gem is not a stand-alone program'

  spec.metadata = {'logstash_plugin' => 'true', 'logstash_group' => 'input'}

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "TODO: Set to 'http://mygemserver.com'"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.13"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.0"

  spec.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"

  spec.add_runtime_dependency 'jmx4r'

  spec.add_development_dependency 'logstash-codec-plain'
  spec.add_development_dependency 'logstash-output-stdout'
end
