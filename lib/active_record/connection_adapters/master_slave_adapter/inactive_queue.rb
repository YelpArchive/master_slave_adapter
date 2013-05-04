require 'thread'

module ActiveRecord
  module ConnectionAdapters
    module MasterSlaveAdapter
      class InactiveQueue
        attr_accessor :pool, :inactive, :connection, :logger, :consumer

        def initialize(pool, logger=nil)
          self.pool     = pool
          self.logger   = logger
          self.inactive = Queue.new
        end

        def start
          self.consumer = Thread.new { queue_consumer }
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
        def queue_consumer
          loop do
            self.connection = inactive.pop

            attempt = begin
              connection.reconnect!
              connection.active?
            rescue Exception => e
              logger.error e if logger
              false
            end

            (attempt ? pool : inactive) << connection

            sleep 5
          end
        end
      end
    end
  end
end
