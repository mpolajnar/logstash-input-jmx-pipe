Gem::Specification.new do |s|
  s.name          = 'logstash-input-jmx_pipe'
  s.version       = '0.1.0'
  s.licenses      = ['Apache License (2.0)']
  s.summary       = 'Retrieve metrics and subscribe to notifications from a single JMX source.'
  s.homepage      = 'https://github.com/mpolajnar/logstash-input-jmx-pipe'
  s.authors       = ['Matija Polajnar, Marand d.o.o.']
  s.email         = 'matija.polajnar@gmail.com'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = {'logstash_plugin' => 'true', 'logstash_group' => 'input'}

  # Gem dependencies
  s.add_runtime_dependency 'logstash-core-plugin-api', '~> 2.0'
  s.add_runtime_dependency 'logstash-codec-plain'
  s.add_runtime_dependency 'jmx4r'
  s.add_development_dependency 'logstash-devutils', '>= 0.0.16'
end
