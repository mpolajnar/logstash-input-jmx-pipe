# encoding: utf-8
require 'forwardable'
require 'spec_helper'
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/codecs/plain'
require 'stud/temporary'
require 'jmx4r'

describe Logstash::Inputs::JmxPipe do
  it "has a version number" do
    expect(Logstash::Inputs::JmxPipe::VERSION).not_to be nil
  end

  it "does something useful" do
    expect(false).to eq(true)
  end
end
