$: << File.expand_path(File.join(File.dirname( __FILE__ ), '..', '..', 'lib'))

require 'rspec'
require 'active_record/connection_adapters/master_slave_adapter/inactive_queue'

describe ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::InactiveQueue do
  let(:connection_pool) { [] }
  let(:connection) { stub(:reconnect! => true, :active? => true) }
  subject { described_class.new(connection_pool, nil, 0) }

  it 'should work with original connection pool' do
    subject.pool.object_id.should == connection_pool.object_id
  end

  context 'when consuming' do
    after :each do
      subject.send :consume
    end

    it 'should try to reconnect to inactive connection' do
      connection.should_receive(:reconnect!)
      connection.should_receive(:active?)
      subject << connection
    end

    it 'should wait between attempts' do
      subject.should_receive(:sleep).with(0)
      subject << connection
    end

    it 'should move connection to connection pool when reconnected' do
      connection_pool.should_receive(:<<).with(connection)
      subject << connection
    end

    it 'should push connection to inactive queue when failed to reconnect' do
      connection.should_receive(:active?).and_return(false)
      subject << connection
      subject.inactive.should_receive(:<<).with(connection)
    end

    it 'should push connection to inactive queue when exception was raised' do
      connection.should_receive(:reconnect!).and_raise(RuntimeError)
      subject << connection
      subject.inactive.should_receive(:<<).with(connection)
    end
  end
end
