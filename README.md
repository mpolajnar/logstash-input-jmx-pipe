# Logstash Plugin

This is a plugin for [Logstash](https://github.com/elastic/logstash).

We did not yet decide on the licence, so only Marand d.o.o. employees and subcontractors are not allowed to run, read or modify it!

## Documentation

TBD

### Running unpublished Plugin in Logstash

#### Run in a local Logstash clone

- Edit Logstash `Gemfile` and add the local plugin path, for example:
```ruby
gem "logstash-input-jmx-pipe", :path => "/your/local/logstash-input-jmx-pipe"
```
- Install plugin
```sh
# Logstash 2.3 and higher
bin/logstash-plugin install --no-verify

# Prior to Logstash 2.3
bin/plugin install --no-verify

```
- Run Logstash with your plugin
```sh
bin/logstash -e 'input {jmx_pipe {}}'
```
At this point any modifications to the plugin code will be applied to this local Logstash setup. After modifying the plugin, simply rerun Logstash.

#### Run in an installed Logstash

You can use the same method to run your plugin in an installed Logstash by editing its `Gemfile` and pointing the `:path` to your local plugin development directory or you can build the gem and install it using:

- Build your plugin gem
```sh
gem build logstash-input-jmx-pipe.gemspec
```
- Install the plugin from the Logstash home
```sh
# Logstash 2.3 and higher
bin/logstash-plugin install --no-verify

# Prior to Logstash 2.3
bin/plugin install --no-verify

```
- Start Logstash and proceed to test the plugin

## Contributing

TBD