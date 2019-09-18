# frozen_string_literal: true

require_relative '../helper'
require 'fluent/plugin/buf_chunkio'
require 'fluent/plugin/output'
require 'timecop'

module FluentPluginChunkioBufferTest
  class DummyOutputPlugin < Fluent::Plugin::Output
    Fluent::Plugin.register_output('buffer_chunkio_output', self)

    config_section :buffer do
      config_set_default :@type, 'chunkio'
    end

    def multi_workers_ready?
      true
    end

    def write(chunk)
      # drop
    end
  end
end

class ChunkioBufferTest < Test::Unit::TestCase
  def gen_metadata(timekey: nil, tag: nil, variables: nil)
    Fluent::Plugin::Buffer::Metadata.new(timekey, tag, variables)
  end

  BUF_PATH = File.expand_path('../tmp/buffer_chunkio', __dir__)
  STREAM_NAME = 'buffer'

  setup do
    Fluent::Test.setup

    @chunkdir = BUF_PATH
    @stream_name = STREAM_NAME
    @d = FluentPluginChunkioBufferTest::DummyOutputPlugin.new

    FileUtils.rm_r(@chunkdir) rescue StandardError
    FileUtils.mkdir_p(@chunkdir)
  end

  teardown do
    FileUtils.rm_r(@chunkdir) rescue StandardError
  end

  def buffer_config(config = {})
    elems = { 'path' => @chunkdir, 'stream_name' => @stream_name }
    config_element('buffer', '', elems.merge(config))
  end

  sub_test_case '#configure' do
    setup do
      @b = Fluent::Plugin::ChunkioBuffer.new
      @b.owner = @d
    end

    test 'path has context, stream_name, file_name' do
      path = File.expand_path('../../tmp/buffer_chunkio2', __dir__)
      stream_name = 'stream_name2'
      @b.configure('path' => path, 'stream_name' => stream_name)
      assert_equal File.join(path, stream_name, 'cio.*.buf'), @b.path
    end

    test 'path can be set filename suffix' do
      @b.configure(buffer_config('file_suffix' => 'log'))
      assert_equal File.join(@chunkdir, @stream_name, 'cio.*.log'), @b.path
    end

    data(
      'path is defined at system' => [
        {},
        File.join(BUF_PATH, 'worker0', 'dummy_output_with_chunkio_buf')
      ],
      'path is defined at both system and buffer' => [
        { 'path' => File.expand_path('../../tmp/buffer_chunkio2', __dir__) },
        File.expand_path('../../tmp/buffer_chunkio2', __dir__)
      ]
    )
    test 'can see the root_dir' do |args|
      opt, expect = args
      Fluent::SystemConfig.overwrite_system_config('root_dir' => @chunkdir) do
        @d.configure(config_element('ROOT', '', '@id' => 'dummy_output_with_chunkio_buf'))
        @b.configure(config_element('buffer', '', { 'stream_name' => @stream_name }.merge(opt)))
      end

      assert_equal File.join(expect, @stream_name, 'cio.*.buf'), @b.path
    end

    test 'add worker direcotry to pass if multiple worker mode' do
      Fluent::SystemConfig.overwrite_system_config('workers' => 2) do
        @b.configure(buffer_config)
      end

      assert_equal File.join(@chunkdir, @stream_name, 'worker0', 'cio.*.buf'), @b.path
    end

    test 'path includes worker id and @id if multiple worker mode and path is defined in system' do
      Fluent::SystemConfig.overwrite_system_config('workers' => 2, 'root_dir' => @chunkdir) do
        @d.configure(config_element('ROOT', '', '@id' => 'dummy_output_with_chunkio_buf'))
        @b.configure(config_element('buffer', '', 'stream_name' => @stream_name))
      end

      assert_equal File.join(@chunkdir, 'worker0', 'dummy_output_with_chunkio_buf', @stream_name, 'cio.*.buf'), @b.path
    end

    data(
      'path is empty' => { 'path' => '', 'stream_name' => 'log' },
      'path is nil' => { 'path' => nil, 'stream_name' => 'log' },
      'stream_name is empty' => { 'path' => BUF_PATH, 'stream_name' => '' },
      'stream_name is nil' => { 'path' => BUF_PATH, 'stream_name' => nil },
      'path includes *' => { 'path' => BUF_PATH + '/cio.*.buf', 'stream_name' => 'log' }
    )
    test 'raise an error when' do |opt|
      assert_raise Fluent::ConfigError do
        @b.configure(opt)
      end
    end
  end

  sub_test_case '#start' do
    setup do
      @path = File.join(@chunkdir, 'start')
      @b = Fluent::Plugin::ChunkioBuffer.new
      @b.owner = @d
    end

    teardown do
      if @d
        @d.stop unless @d.stopped?
        @d.before_shutdown unless @d.before_shutdown?
        @d.shutdown unless @d.shutdown?
        @d.after_shutdown unless @d.after_shutdown?
        @d.close unless @d.closed?
        @d.terminate unless @d.terminated?
      end
      FileUtils.rm_r(@path) rescue StandardError
    end

    test 'create directory' do
      assert_false File.exist?(@path)
      @b.configure(buffer_config('path' => @path))
      @b.start
      assert File.exist?(@path)
    end

    test 'create directory with specify permission' do
      assert_false File.exist?(@path)
      @b.configure(buffer_config('path' => @path, 'dir_permission' => '700'))
      @b.start
      assert File.stat(File.join(@path, @stream_name)).mode.to_s(8).end_with?('700')
    end
  end

  sub_test_case '#resume' do
    setup do
      @path = File.join(@chunkdir, 'resume')
      FileUtils.rm_r(@path) rescue StandardError
      @b = Fluent::Plugin::ChunkioBuffer.new
      @b.owner = @d
    end

    teardown do
      if @b
        @b.stop unless @b.stopped?
        @b.before_shutdown unless @b.before_shutdown?
        @b.shutdown unless @b.shutdown?
        @b.after_shutdown unless @b.after_shutdown?
        @b.close unless @b.closed?
        @b.terminate unless @b.terminated?
      end

      FileUtils.rm_r(@path) rescue StandardError
    end

    test 'returns empty if there is no file to load' do
      @b.configure(buffer_config('path' => @path))
      @b.start
      stage = @b.stage
      queue = @b.queue

      assert_equal({}, stage)
      assert_equal [], queue
    end

    def assert_chunk_equal(execpted, actual)
      assert_equal execpted.unique_id, actual.unique_id
      assert_equal execpted.state, actual.state
      assert_equal execpted.read, actual.read
      assert_equal execpted.bytesize, actual.bytesize
      assert_equal execpted.size, actual.size
      assert_equal execpted.modified_at, actual.modified_at
      assert_equal execpted.created_at, actual.created_at
      assert_equal execpted.metadata, actual.metadata
    end

    sub_test_case 'there are some files chunks to load' do
      setup do
        Timecop.freeze(Time.parse('2019-09-11 16:59:59 +0000'))
        @chunkio = ChunkIO.new(context_path: @path, stream_name: @stream_name)
        @chunk_path = File.join(@path, @stream_name, 'cio.*.buf')
        @data = { 'k1' => 'v1', 'k2' => 'v2' }

        # util
        @create_chunk = -> (meta, data: nil, chunkio: @chunkio, chunk_path: @chunk_path) {
          c = Fluent::Plugin::Buffer::ChunkioChunk.new(meta, chunk_path, :create, chunk: chunkio)
          if data
            c.append(data)
            c.commit
          end
          c
        }
        @create_staged_chunk = ->(meta, data = nil) { @create_chunk.call(meta, data).tap { |c| c.staged! } }
        @create_queued_chunk = ->(meta, data = nil) { @create_staged_chunk.call(meta, data).tap { |c| c.enqueued! } }

        metadata1 = gen_metadata(tag: 'test1', timekey: Time.now.to_i)
        @chunk1 = @create_staged_chunk.call(metadata1, data: [@data.to_json])
        metadata2 = gen_metadata(tag: 'test1', timekey: Time.now.to_i + 1)
        @chunk2 = @create_queued_chunk.call(metadata2, data: [@data.merge('k2' => 'v3').to_json])
      end

      test 'load staged and queued files' do
        @b.configure(buffer_config('path' => @path))
        @b.start # #resume is called
        stage = @b.stage
        queue = @b.queue

        assert_equal 1, stage.size
        assert_equal 1, queue.size
        assert_chunk_equal @chunk1, stage[@chunk1.metadata]
        assert_chunk_equal @chunk2, queue[0]
      end

      test 'load files which includes placeholders' do
        chunkio = ChunkIO.new(context_path: @path, stream_name: '${stream_name}')
        chunk_path = File.join(@path, '${stream_name}', 'cio.*.buf')
        c = @create_staged_chunk.call(gen_metadata, data: [@data.to_json], chunk_path: chunk_path, chunkio: chunkio)

        @b.configure(buffer_config('path' => @path, 'stream_name' => '${stream_name}'))
        @b.start # #resume is called
        stage = @b.stage
        queue = @b.queue

        assert_equal 1, stage.size
        assert_equal 0, queue.size
        assert_chunk_equal c, stage[c.metadata]
      end

      data(
        'stream_name is wrong' => ['other_stream', 'cio.*.buf'],
        'suffix is wrong' => [STREAM_NAME, 'cio.*.log'],
      )
      test 'ignore files if path is not related' do |args|
        stream_name, file_name = args
        chunkio = ChunkIO.new(context_path: @path, stream_name: stream_name)
        chunk_path = File.join(@path, stream_name, file_name)
        _ = @create_staged_chunk.call(gen_metadata, data: [@data.to_json], chunk_path: chunk_path, chunkio: chunkio)

        @b.configure(buffer_config('path' => @path))
        @b.start # #resume is called
        stage = @b.stage
        queue = @b.queue

        assert_equal 1, stage.size
        assert_equal 1, queue.size
        assert_chunk_equal @chunk1, stage[@chunk1.metadata]
        assert_chunk_equal @chunk2, queue[0]
      end

      test '#resume returns queued chunks ordered by last modified time (FIFO)' do
        c1 = @create_chunk.call(gen_metadata)
        c2 = @create_chunk.call(gen_metadata)
        c1.append([@data.to_json])
        c2.append([@data.to_json])

        # c2 -> c1
        Timecop.freeze(Time.parse('2019-09-11 17:59:59 +0000'))
        c2.commit
        Timecop.freeze(Time.parse('2019-09-11 18:59:59 +0000'))
        c1.commit

        c1.staged!
        c1.enqueued!
        c2.staged!
        c2.enqueued!

        @b.configure(buffer_config('path' => @path))
        @b.start # #resume is called
        stage = @b.stage
        queue = @b.queue

        assert_equal 1, stage.size
        assert_equal 3, queue.size
        assert_true(c2.modified_at <= c1.modified_at)
        assert_equal [@chunk2, c2, c1].map(&:unique_id), queue.map(&:unique_id)
      end

      sub_test_case 'when invalid chunk file' do
        test 'which is empty file' do
          File.open(@chunk_path.gsub('*', '591b6e961076a0d6ca67662c2a93b60f'), 'wb') { |f| } # create staged empty chunk file

          @b.configure(buffer_config('path' => @path))
          @b.start # #resume is called
          stage = @b.stage
          queue = @b.queue

          assert_equal 1, stage.size
          assert_equal 1, queue.size
          assert_chunk_equal @chunk1, stage[@chunk1.metadata]
          assert_chunk_equal @chunk2, queue[0]
        end

        test 'which is buf_file chunk format' do
          File.open(@chunk_path.gsub('*', '591b6e961076a0d6ca67662c2a93b60g'), 'wb') do |f|
            f.write ['test', Time.now.to_i, { 'message' => 'yay' }].to_json + "\n"
          end

          @b.configure(buffer_config('path' => @path))
          @b.start # #resume is called
          stage = @b.stage
          queue = @b.queue

          assert_equal 1, stage.size
          assert_equal 1, queue.size
          assert_chunk_equal @chunk1, stage[@chunk1.metadata]
          assert_chunk_equal @chunk2, queue[0]
        end
      end

      sub_test_case 'when multiple workers modes' do
        test 'worker0 reads the existing files which are located in parent directory' do
          Fluent::SystemConfig.overwrite_system_config('workers' => 2) do
            @b.configure(buffer_config('path' => @path))
          end
          @b.start

          stage = @b.stage
          queue = @b.queue

          assert_equal 1, stage.size
          assert_equal 1, queue.size
          assert_chunk_equal @chunk1, stage[@chunk1.metadata]
          assert_chunk_equal @chunk2, queue[0]
        end

        test 'load files under direcotry named worker${n}' do
          chunkio0 = ChunkIO.new(context_path: @path, stream_name: "#{@stream_name}/worker0")
          chunk_path0 = File.join(@path, "#{@stream_name}/worker0", 'cio.*.buf')
          c0 = @create_staged_chunk.call(gen_metadata(tag: 'workers'), data: [@data.to_json], chunk_path: chunk_path0, chunkio: chunkio0)

          # should be ignored
          chunkio1 = ChunkIO.new(context_path: @path, stream_name: "#{@stream_name}/worker1")
          chunk_path1 = File.join(@path, "#{@stream_name}/worker1", 'cio.*.buf')
          _ = @create_staged_chunk.call(gen_metadata, data: [@data.to_json], chunk_path: chunk_path1, chunkio: chunkio1)

          Fluent::SystemConfig.overwrite_system_config('workers' => 2) do
            @b.configure(buffer_config('path' => @path))
          end
          @b.start

          stage = @b.stage
          queue = @b.queue

          assert_equal 2, stage.size
          assert_equal 1, queue.size

          assert_chunk_equal @chunk1, stage[@chunk1.metadata]
          assert_chunk_equal c0, stage[c0.metadata]
          assert_chunk_equal @chunk2, queue[0]
        end
      end
    end
  end
end
