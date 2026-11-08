# frozen_string_literal: true

require_relative "lib/vexo/version"

Gem::Specification.new do |spec|
  spec.name = "vexo"
  spec.version = Vexo::VERSION
  spec.authors = ["vexo"]
  spec.summary = "Send server-side events and identifies to Vexo from Ruby."
  spec.homepage = "https://docs.vexo.co/server-side/capture"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.0"
  spec.files = Dir["lib/**/*.rb", "README.md", "LICENSE"]
  spec.metadata = { "documentation_uri" => spec.homepage, "rubygems_mfa_required" => "true" }
end
