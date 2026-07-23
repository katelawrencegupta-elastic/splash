# frozen_string_literal: true

require "logstash/inputs/base"
require "logstash/namespace"
require "socket"
require "timeout"
require_relative "s2s/decoder"

# Splunk cooked S2S TCP terminator.
#
# Owns the TCP socket so capability replies can be written back to the
# forwarder (stock tcp input + ruby filter cannot do this). Decoded events
# enter the pipeline already shaped for the classify sidecar.
class LogStash::Inputs::S2s < LogStash::Inputs::Base
  config_name "s2s"

  default :codec, "plain"

  config :host, validate: :string, default: "0.0.0.0"
  config :port, validate: :number, required: true
  config :max_message_size, validate: :number, default: LogStash::S2s::DEFAULT_MAX_MESSAGE_SIZE
  config :tcp_keep_alive, validate: :boolean, default: true
  config :event_tags, validate: :array, default: %w[s2s_decoded splunk_tcp_39998]

  def register
    @server = nil
    @client_threads = []
    @client_threads_lock = Mutex.new
    @logger.info("S2S input configured", host: @host, port: @port)
  end

  def run(queue)
    @server = TCPServer.new(@host, @port)
    @logger.info("S2S listening", address: "#{@host}:#{@port}")

    until stop?
      begin
        client = accept_with_timeout
        next if client.nil?

        configure_socket(client)
        thread = Thread.new(client) do |sock|
          handle_client(sock, queue)
        ensure
          @client_threads_lock.synchronize { @client_threads.delete(Thread.current) }
        end
        @client_threads_lock.synchronize { @client_threads << thread }
      rescue IOError, Errno::EBADF => e
        break if stop?

        @logger.warn("S2S accept IO error", exception: e)
      rescue StandardError => e
        @logger.error("S2S accept failed", exception: e, backtrace: e.backtrace.take(5))
        sleep 0.2
      end
    end
  ensure
    close_server
    join_clients
  end

  def stop
    close_server
  end

  private

  def accept_with_timeout
    Timeout.timeout(1) { @server.accept }
  rescue Timeout::Error
    nil
  end

  def configure_socket(sock)
    sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_KEEPALIVE, 1) if @tcp_keep_alive
    sock.binmode
  rescue StandardError
    nil
  end

  def handle_client(sock, queue)
    peer = "unknown"
    session = nil
    peer = begin
      addr = sock.peeraddr
      "#{addr[3]}:#{addr[1]}"
    rescue StandardError
      "unknown"
    end
    session = LogStash::S2s::Session.new(
      max_message_size: @max_message_size,
      extra_tags: @event_tags
    )
    @logger.info("S2S connection", peer: peer)

    until stop?
      data = sock.readpartial(65_536)
      break if data.nil? || data.empty?

      events = session.feed(data)
      events.each { |hash| push_event(queue, hash) }
      write_replies(sock, session)
    end

    session.flush.each { |hash| push_event(queue, hash) }
    write_replies(sock, session)
  rescue EOFError, Errno::ECONNRESET, Errno::EPIPE, IOError
    # client closed
  rescue StandardError => e
    @logger.warn("S2S client error", peer: peer, exception: e)
  ensure
    stats = session&.stats
    @logger.info(
      "S2S connection closed",
      peer: peer,
      frames_ok: stats&.frames_ok,
      events: stats&.events_emitted,
      caps_replied: stats&.capabilities_replied
    )
    begin
      sock.close
    rescue StandardError
      nil
    end
  end

  def write_replies(sock, session)
    replies = session.take_replies!
    return if replies.empty?

    replies.each { |blob| sock.write(blob) }
    sock.flush
  rescue StandardError => e
    @logger.warn("S2S capability reply failed", exception: e)
  end

  def push_event(queue, hash)
    event = LogStash::Event.new(hash)
    decorate(event)
    queue << event
  end

  def close_server
    return unless @server

    begin
      @server.close
    rescue StandardError
      nil
    end
    @server = nil
  end

  def join_clients
    threads = @client_threads_lock.synchronize { @client_threads.dup }
    threads.each do |t|
      t.join(2)
    rescue StandardError
      nil
    end
  end
end
