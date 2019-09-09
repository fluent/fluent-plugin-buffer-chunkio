# frozen_string_literal: true

require 'bundler/gem_tasks'
require 'rake/testtask'

Rake::TestTask.new(:test) do |t|
  t.libs << 'test'
  t.libs << 'lib'
  t.test_files = FileList['test/**/test_*.rb']

  t.verbose = true
  t.warning = true
  t.ruby_opts = ['-Eascii-8bit:ascii-8bit']
end

task :default => :test
