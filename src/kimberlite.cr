require "logger"
require "socket"

module Kimberlite
  extend self

  # Package version
  VERSION = "0.1.0"

  # Data types stored
  # TODO: Currently only stores Strings
  alias Value = String | Int64 | Float64

  # Configure the state of the store
  @@server = TCPServer.new("localhost", 7675)
  @@store = Hash(String, Value).new
  @@logger = Logger.new(STDOUT)

  # Configure the store state
  def configure
    @@logger.level = Logger::INFO
  end

  # Start the TCP server
  def start_server
    @@logger.info("Started server at kmb://localhost:7675")
    # Accpet incoming connections and spawn off client workers
    while client = @@server.accept?
      spawn handle_client client
    end
  end

  # Send a successful action response
  def successful_action(client, *message)
    if message.nil?
      client << "OK\n"
    else
      client << "OK #{message.join(" ")}\n"
    end
  end

  # Handle a single client connection
  def handle_client(client : TCPSocket)
    @@logger.info("Opened connection to #{client.remote_address}")
    # Start the connection loop
    loop do
      # Error boundary for commands
      begin
        # Get the message being sent
        message = client.gets
        # If the message has content
        unless message.nil?
          # Split the command into it's arguments
          command = message.split " "

          # TODO: Abstract this if statement into a command name / arity check
          # Call the proper commands, if successful return an OK response
          if command[0] == "SET" && command.size == 3 # SET
            @@store[command[1]] = command[2]
            successful_action client
          elsif command[0] == "GET" && command.size == 2 # GET
            if @@store.has_key? command[1]
              successful_action client, "#{command[1]} #{@@store[command[1]]}"
            else
              raise "Cannot find key \"#{@@store[command[1]]}\""
            end
          elsif command[0] == "QUIT" && command.size == 1 # QUIT
            @@logger.info "Client closed connection with #{client.remote_address}"
            client.close
            break
          else # Handle invalid commands
            raise "Invalid command #{command[0]}"
          end
        end
      # Bubble up exceptions and return an ERR response
      rescue exception
        client << "ERR \"#{exception}\"\n"
      end

      # End connection if the state changed during a command
      if client.closed?
        @@logger.info("Connection unexpectedly closed from #{client.remote_address}") if client.closed?
        break
      end
    end
  end

  start_server
end
