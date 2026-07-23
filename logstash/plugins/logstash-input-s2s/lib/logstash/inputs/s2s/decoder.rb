# frozen_string_literal: true

# Splunk cooked S2S byte-stream decoder (signature + framed messages).
# Port of splash/s2s Python library for use inside Logstash.

module LogStash
  module S2s
    class KvError < StandardError; end

    SIGNATURE_SIZE = 400
    SIG_BANNER_LEN = 128
    SIG_SERVER_NAME_LEN = 256
    SIG_MGMT_PORT_LEN = 16
    COOKED_BANNER_V2 = "--splunk-cooked-mode-v2--".b
    COOKED_BANNER_V3 = "--splunk-cooked-mode-v3--".b
    DEFAULT_MAX_MESSAGE_SIZE = 16 * 1024 * 1024

    DEFAULT_CAP_RESPONSE =
      "cap_response=success;cap_flush_key=false;idx_can_send_hb=false;" \
      "idx_can_recv_token=false;request_certificate=false;v4=false;" \
      "channel_limit=300;pl=0"

    KNOWN_KEYS = %w[
      _raw host source sourcetype index _time _orphan _done
      _MetaData:Index MetaData:Host MetaData:Source MetaData:Sourcetype
      __s2s_capabilities __s2s_control_msg
    ].freeze

    module Binary
      module_function

      def u32be(buf, offset)
        raise KvError, "truncated u32 at #{offset}" if offset + 4 > buf.bytesize

        buf.byteslice(offset, 4).unpack1("N")
      end

      def pack_u32be(value)
        [value].pack("N")
      end
    end

    class Stats
      attr_accessor :handshake_seen, :frames_ok, :frames_bad, :frames_oversized,
                    :events_emitted, :bytes_consumed, :capabilities_replied,
                    :protocol_version

      def initialize
        @handshake_seen = 0
        @frames_ok = 0
        @frames_bad = 0
        @frames_oversized = 0
        @events_emitted = 0
        @bytes_consumed = 0
        @capabilities_replied = 0
        @protocol_version = 0
      end
    end

    class Message
      attr_accessor :index, :host, :source, :sourcetype, :raw, :time, :fields

      def initialize
        @index = ""
        @host = ""
        @source = ""
        @sourcetype = ""
        @raw = ""
        @time = ""
        @fields = {}
      end
    end

    module Strings
      module_function

      def encode(value)
        data = value.to_s.encode("UTF-8")
        Binary.pack_u32be(data.bytesize + 1) + data.b + "\x00".b
      end

      def decode_at(buf, offset)
        length = Binary.u32be(buf, offset)
        offset += 4
        raise KvError, "invalid string length #{length}" if length < 1

        end_off = offset + length
        raise KvError, "string exceeds buffer" if end_off > buf.bytesize
        raise KvError, "missing NUL terminator" if buf.getbyte(end_off - 1) != 0

        raw = buf.byteslice(offset, length - 1)
        [raw.force_encoding("UTF-8").encode("UTF-8", invalid: :replace, undef: :replace), end_off]
      end

      def decode_kv_at(buf, offset)
        key, offset = decode_at(buf, offset)
        value, offset = decode_at(buf, offset)
        [key, value, offset]
      end
    end

    module Caps
      module_function

      def build_response(client_caps)
        caps = {}
        client_caps.to_s.split(";").each do |part|
          k, v = part.split("=", 2)
          caps[k] = v if k && v
        end
        pl = caps["pl"] || "0"
        v4 = caps["v4"] || "0"
        v4_flag = %w[1 true True].include?(v4) ? "true" : "false"
        "cap_response=success;cap_flush_key=false;idx_can_send_hb=false;" \
          "idx_can_recv_token=false;request_certificate=false;" \
          "v4=#{v4_flag};channel_limit=300;pl=#{pl}"
      end

      def encode_reply(response = DEFAULT_CAP_RESPONSE)
        encode_control_message("__s2s_control_msg", response)
      end

      def encode_control_message(key, value)
        kv = Strings.encode(key) + Strings.encode(value)
        maps = 1
        trailer = Strings.encode("_raw")
        body = Binary.pack_u32be(maps) + kv + Binary.pack_u32be(0) + trailer
        Binary.pack_u32be(body.bytesize) + body
      end
    end

    module MessageCodec
      module_function

      def strip_prefix(value, prefix)
        value.start_with?(prefix) ? value[prefix.length..] : value
      end

      def apply_kv(msg, key, value)
        case key
        when "_MetaData:Index"
          msg.index = value
        when "MetaData:Host"
          msg.host = strip_prefix(value, "host::")
        when "MetaData:Source"
          msg.source = strip_prefix(value, "source::")
        when "MetaData:Sourcetype"
          msg.sourcetype = strip_prefix(value, "sourcetype::")
        when "_time"
          msg.time = value
        when "_done"
          nil
        when "_raw"
          msg.raw = value
        else
          msg.fields[key] = value
        end
      end

      def decode_body(body)
        raise KvError, "body too short" if body.bytesize < 4

        maps = Binary.u32be(body, 0)
        offset = 4
        msg = Message.new
        maps.times do
          key, value, offset = Strings.decode_kv_at(body, offset)
          apply_kv(msg, key, value)
        end
        raise KvError, "missing _raw padding" if offset + 4 > body.bytesize

        padding = Binary.u32be(body, offset)
        offset += 4
        raise KvError, "unexpected padding #{padding}" unless padding.zero?

        trailer, = Strings.decode_at(body, offset)
        raise KvError, "unexpected trailer #{trailer.inspect}" unless trailer == "_raw"

        msg
      end

      # Returns [message_or_nil, consumed, error_or_nil]
      def try_read(buf, max_size:)
        return [nil, 0, nil] if buf.bytesize < 4

        size = Binary.u32be(buf, 0)
        return [nil, 1, "oversized message size=#{size}"] if size > max_size
        return [nil, 1, "undersized message size=#{size}"] if size < 4

        total = 4 + size
        return [nil, 0, nil] if buf.bytesize < total

        begin
          msg = decode_body(buf.byteslice(4, size))
          [msg, total, nil]
        rescue KvError => e
          [nil, 1, e.message]
        end
      end

      def to_event_hash(msg, extra_tags:)
        fields = {
          "host" => msg.host,
          "source" => msg.source,
          "sourcetype" => msg.sourcetype,
          "index" => msg.index,
          "_raw" => msg.raw
        }
        fields["_time"] = msg.time unless msg.time.empty?
        fields.merge!(msg.fields)

        message = fields["_raw"].to_s
        message = fields["message"].to_s if message.empty?
        event = {
          "host" => fields["host"].to_s,
          "source" => fields["source"].to_s,
          "sourcetype" => fields["sourcetype"].to_s,
          "splunk_index" => fields["index"].to_s,
          "message" => message,
          "tags" => extra_tags.dup
        }
        if fields.key?("_time") && !fields["_time"].to_s.empty?
          begin
            event["_time"] = Float(fields["_time"])
          rescue ArgumentError, TypeError
            event["_time"] = fields["_time"]
          end
        end
        extras = fields.reject { |k, _| KNOWN_KEYS.include?(k) }
        event["s2s"] = { "fields" => extras } unless extras.empty?
        event
      end
    end

    class Session
      attr_reader :stats, :replies

      def initialize(max_message_size: DEFAULT_MAX_MESSAGE_SIZE, extra_tags: %w[s2s_decoded splunk_tcp_39998])
        @max_message_size = max_message_size
        @extra_tags = extra_tags
        @buf = "".b
        @handshake_done = false
        @replies = []
        @stats = Stats.new
      end

      def take_replies!
        out = @replies
        @replies = []
        out
      end

      def feed(data)
        return [] if data.nil? || data.empty?

        chunk = data.b
        @buf << chunk
        @stats.bytes_consumed += chunk.bytesize
        drain
      end

      def flush
        events = drain
        @buf.clear if @buf.bytesize.positive?
        events
      end

      private

      def drain
        events = []
        loop do
          unless @handshake_done
            break unless try_consume_signature

            next
          end

          result = try_consume_message
          break if result == :need_more
          next if result.nil?

          events << result
        end
        events
      end

      def try_consume_signature
        if @buf.bytesize < SIGNATURE_SIZE
          peek = @buf
          if COOKED_BANNER_V3.start_with?(peek) || COOKED_BANNER_V2.start_with?(peek) ||
             peek.start_with?(COOKED_BANNER_V3.byteslice(0, [8, peek.bytesize].min)) ||
             peek.start_with?(COOKED_BANNER_V2.byteslice(0, [8, peek.bytesize].min))
            return false
          end
          return false if @buf.empty?

          @handshake_done = true
          return true
        end

        banner = @buf.byteslice(0, SIG_BANNER_LEN).sub(/\x00+\z/, "")
        unless [COOKED_BANNER_V3, COOKED_BANNER_V2].include?(banner)
          idx = @buf.index(COOKED_BANNER_V3) || @buf.index(COOKED_BANNER_V2)
          if idx && idx.positive? && idx <= 64 && @buf.bytesize >= idx + SIGNATURE_SIZE
            @buf = @buf.byteslice(idx..)
            return true
          end
          @handshake_done = true
          return true
        end

        version = banner == COOKED_BANNER_V3 ? 3 : 2
        @buf = @buf.byteslice(SIGNATURE_SIZE..)
        @handshake_done = true
        @stats.handshake_seen += 1
        @stats.protocol_version = version
        true
      end

      def try_consume_message
        msg, consumed, err = MessageCodec.try_read(@buf, max_size: @max_message_size)
        if consumed.zero? && err.nil? && msg.nil?
          return :need_more
        end
        if err
          if err.include?("oversized")
            @stats.frames_oversized += 1
          else
            @stats.frames_bad += 1
          end
          skip = [consumed, 1].max
          @buf = @buf.byteslice(skip..)
          return nil
        end

        @buf = @buf.byteslice(consumed..)
        @stats.frames_ok += 1

        if msg.fields.key?("__s2s_capabilities")
          caps = msg.fields["__s2s_capabilities"].to_s
          @replies << Caps.encode_reply(Caps.build_response(caps))
          @stats.capabilities_replied += 1
          return nil if msg.raw.empty?
        end

        return nil if msg.raw.empty?

        @stats.events_emitted += 1
        MessageCodec.to_event_hash(msg, extra_tags: @extra_tags)
      end
    end
  end
end
