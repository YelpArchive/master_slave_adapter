require 'thread'

module ActiveRecord
  module ConnectionAdapters
    module MasterSlaveAdapter
      class InactiveQueue
        attr_accessor :pool, :inactive, :connection, :logger, :consumer, :freq

        def initialize(pool, logger=nil, freq=5)
          self.pool     = pool
          self.logger   = logger
          self.inactive = Queue.new
          self.freq     = freq
        end

        def start
          self.consumer = Thread.new { loop { consume } }
        end

        def stop
          consumer.exit if consumer
          pool << connection if connection && !pool.include?(connection)
          pool << inactive.pop until inactive.empty?
        end

        def <<(connection)
          inactive << connection
        end

        private

        # every 5 second pop connection from inactive queue and attempt to
        # reconnect
        #
        # on failure push connection back onto inactive queue, on success add
        # connection back to pool
        def consume
          self.connection = inactive.pop
          logger.error("reconnecting to #{connection.send :connection_info}")

          attempt = begin
            connection.reconnect!
            connection.active?
          rescue Exception => e
            logger.error e if logger
            false
          end

          (attempt ? pool : inactive) << connection

          sleep freq
        end
      end
    end
  end
end
