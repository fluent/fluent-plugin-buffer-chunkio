# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = 'fluent-plugin-buffer-chunkio'
  spec.version       = '0.1.0'
  spec.authors       = ['Yuta Iwama']
  spec.email         = ['ganmacs@gmail.com']

  spec.summary       = 'buffer plugin using chunkio for fluentd'
  spec.description   = 'buffer plugin using chunkio for fluentd'
  spec.homepage      = 'https://github.com/fluent/fluent-plugin-buffer-chunkio'
  spec.license       = 'Apache-2.0'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_runtime_dependency 'fluentd', '>= 0.14.0'
  spec.add_runtime_dependency 'chunkio', '>= 0.1.2'

  spec.add_development_dependency 'bundler'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'rr'
  spec.add_development_dependency 'test-unit'
  spec.add_development_dependency 'test-unit-rr'
  spec.add_development_dependency 'timecop'
end
