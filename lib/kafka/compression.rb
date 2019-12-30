# frozen_string_literal: true

require "kafka/snappy_codec"
require "kafka/gzip_codec"

unless RUBY_PLATFORM =~ /java/i
  require "kafka/lz4_codec"
  require "kafka/zstd_codec"
end

module Kafka
  module Compression
    CODECS_BY_NAME = {
      :snappy => SnappyCodec.new,
    }.tap do |h|
      unless RUBY_PLATFORM =~ /java/i
        h[:gzip] = GzipCodec.new
        h[:lz4] = LZ4Codec.new
        h[:zstd] = ZstdCodec.new
      end
    end.freeze

    CODECS_BY_ID = CODECS_BY_NAME.each_with_object({}) do |(_, codec), hash|
      hash[codec.codec_id] = codec
    end.freeze

    def self.codecs
      CODECS_BY_NAME.keys
    end

    def self.find_codec(name)
      codec = CODECS_BY_NAME.fetch(name) do
        raise "Unknown compression codec #{name}"
      end

      codec.load

      codec
    end

    def self.find_codec_by_id(codec_id)
      codec = CODECS_BY_ID.fetch(codec_id) do
        raise "Unknown codec id #{codec_id}"
      end

      codec.load

      codec
    end
  end
end
