require 'integration/support/mysql_setup_helper'

shared_examples_for "a MySQL MasterSlaveAdapter" do
  include MysqlSetupHelper

  let(:configuration) do
    {
      :adapter => 'master_slave',
      :connection_adapter => connection_adapter,
      :username => 'root',
      :database => 'master_slave_adapter',
      :master => {
        :host => '127.0.0.1',
        :port => port(:master, 1),
      },
      :slaves => [
        { :host => '127.0.0.1',
          :port => port(:slave, 1) },
        { :host => '127.0.0.1',
          :port => port(:slave, 2) } ],
    }
  end

  let(:test_table) { MysqlSetupHelper::TEST_TABLE }

  let(:logger) { nil }

  def connection
    ActiveRecord::Base.connection
  end

  def should_read_from(role, index=nil)
    server = server_id(index.nil? ? :master : host, index || 1)
    query  = "SELECT @@Server_id as Value"

    [ connection.select_all(query).first["Value"],
      connection.select_one(query)["Value"],
      connection.select_rows(query).first.first,
      connection.select_value(query),
      connection.select_values(query).first ].each do |id|
      if role == :master || !index.nil?
        # ensure equal id when master or slave with index
        id.to_s.should == server
      else role == :slave
        id.to_s.to_i.should > server.to_i
      end
    end
  end

  before(:all) do
    setup

    start_master
    start_slave 1
    start_slave 2

    configure_master
    configure_slave 1
    configure_slave 2

    start_replication 1
    start_replication 2
  end

  after(:all) do
    stop_master
    stop_slave 1
    stop_slave 2
  end

  before do
    ActiveRecord::Base.establish_connection(configuration)
    ActiveRecord::Base.logger = logger
    ActiveRecord::Base.connection.should be_active
  end

  it "connects to the database" do
    expect { ActiveRecord::Base.connection }.to_not raise_error
  end

  context "given a debug logger" do
    let(:logger) do
      logger = []
      def logger.debug(*args)
        push(args.join)
      end
      def logger.debug?
        true
      end

      logger
    end

    it "logs the connection info" do
      ActiveRecord::Base.connection.select_value("SELECT 42")

      logger.last.should =~ /\[slave:127.0.0.1:\d+\] SQL .*SELECT 42/
    end
  end

  context "when asked for master" do
    it "reads from master" do
      ActiveRecord::Base.with_master do
        should_read_from :master
      end
    end
  end

  context "when asked for slave" do
    it "reads from slave" do
      ActiveRecord::Base.with_slave do
        should_read_from :slave
      end
    end
  end

  context "when asked for consistency" do
    context "given slave is fully synced" do
      before do
        wait_for_replication_sync 1
        wait_for_replication_sync 2
      end

      it "reads from slave" do
        ActiveRecord::Base.with_consistency(connection.master_clock) do
          should_read_from :slave
        end
      end
    end

    context "given slave lags behind" do
      before do
        stop_replication 1
        stop_replication 2
        move_master_clock
      end

      after do
        start_replication 1
        start_replication 2
      end

      it "reads from master" do
        ActiveRecord::Base.with_consistency(connection.master_clock) do
          should_read_from :master
        end
      end

      context "and slave catches up" do
        before do
          start_replication 1
          start_replication 2
          wait_for_replication_sync 1
          wait_for_replication_sync 2
        end

        it "reads from slave" do
          ActiveRecord::Base.with_consistency(connection.master_clock) do
            should_read_from :slave
          end
        end
      end
    end

    context "given we always wait for slave to catch up and be consistent" do
      before do
        start_replication 1
        start_replication 2
      end

      it "should always read from slave" do
        wait_for_replication_sync 1
        wait_for_replication_sync 2

        ActiveRecord::Base.with_consistency(connection.master_clock) do
          should_read_from :slave
        end
        move_master_clock

        wait_for_replication_sync 1
        wait_for_replication_sync 2

        ActiveRecord::Base.with_consistency(connection.master_clock) do
          should_read_from :slave
        end
      end
    end
  end

  context "given master goes away in between queries" do
    let(:query) { "INSERT INTO #{test_table} (message) VALUES ('test')" }

    after do
      start_master
    end

    it "raises a MasterUnavailable exception" do
      expect do
        ActiveRecord::Base.connection.insert(query)
      end.to_not raise_error

      stop_master

      expect do
        ActiveRecord::Base.connection.insert(query)
      end.to raise_error(ActiveRecord::MasterUnavailable)
    end
  end

  context "given master is not available" do
    before do
      stop_master
    end

    after do
      start_master
    end

    context "when asked for master" do
      it "fails" do
        expect do
          ActiveRecord::Base.with_master { should_read_from :master }
        end.to raise_error(ActiveRecord::MasterUnavailable)
      end
    end

    context "when asked for slave" do
      it "reads from slave" do
        ActiveRecord::Base.with_slave do
          should_read_from :slave
        end
      end
    end
  end

  context "given slave is not available" do
    let(:queue) { connection.instance_variable_get('@inactive_queue').inactive }

    before do
      stop_slave 1
      stop_slave 2
    end

    after do
      start_slave 1
      start_slave 2
    end

    context "when asked for slave" do
      it "fails" do
        expect do
          should_read_from :slave
        end.to raise_error(ActiveRecord::SlaveUnavailable)
      end

      it "moves slave into inactive queue" do
        should_read_from :slave rescue nil
        queue.size.should be(1)
      end

      it "attempts to reconnect" do
        all_connections = connection.connections
        should_read_from :slave rescue nil
        failed_connections = all_connections - connection.connections
        failed_connections.size.should be(1)
        failed_connections.first.should_receive(:reconnect!)
        sleep(5)
      end

      it "brings slave back when server available" do
        all_connections = connection.connections
        should_read_from :slave rescue nil
        failed_connection = (all_connections - connection.connections).first
        slave_port = failed_connection.instance_variable_get('@config')[:port]
        start_slave(slave_port - master_port)
        sleep(5)
        connection.connections.should include(failed_connection)
      end
    end
  end
end
