require 'fileutils'
require 'timeout'

module MysqlSetupHelper
  TEST_TABLE  = "master_slave_adapter.master_slave_test"

  def master_id(*args); "1"; end
  def slave_id(index=1); (master_id.to_i + index).to_s; end

  def master_port(*args); 3310; end
  def slave_port(index=1); master_port + index; end

  def port(identifier, index=1)
    send("#{identifier}_port", index)
  end

  def server_id(identifier, index=1)
    send("#{identifier}_id", index)
  end

  def start_replication(index=1)
    execute("start slave", :slave, index)
  end

  def stop_replication(index=1)
    execute("stop slave", :slave, index)
  end

  def move_master_clock
    execute("insert into #{TEST_TABLE} (message) VALUES ('test')", :master)
  end

  def wait_for_replication_sync
    Timeout.timeout(5) do
      until slave_status == master_status; end
    end
  rescue Timeout::Error
    raise "Replication synchronization failed"
  end

  def configure
    execute(<<-EOS, :master)
      SET sql_log_bin = 0;
      create user 'slave'@'localhost' identified by 'slave';
      grant replication slave on *.* to 'slave'@'localhost';
      create database master_slave_adapter;
      SET sql_log_bin = 1;
    EOS

    execute(<<-EOS, :slave, 1)
      change master to master_user = 'slave',
             master_password = 'slave',
             master_port = #{port(:master)},
             master_host = 'localhost';
      create database master_slave_adapter;
    EOS

    execute(<<-EOS, :master)
      CREATE TABLE #{TEST_TABLE} (
        id int(11) NOT NULL AUTO_INCREMENT,
        message text COLLATE utf8_unicode_ci,
        created_at datetime DEFAULT NULL,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
    EOS
  end

  def setup_for(name, index=1)
    path        = location(name, index)
    config_path = File.join(path, "my.cnf")
    base_dir    = File.dirname(File.dirname(`which mysql_install_db`))

    FileUtils.rm_rf(path)
    FileUtils.mkdir_p(path)
    File.open(config_path, "w") { |file| file << config(name, index) }

    `mysql_install_db --defaults-file='#{config_path}' --basedir='#{base_dir}' --user=''`
  end

  def setup
    setup_for(:master)
    setup_for(:slave)
  end

  def start_master
    start(:master)
  end

  def stop_master
    stop(:master)
  end

  def start_slave(index=1)
    start(:slave, index)
  end

  def stop_slave(index=1)
    stop(:slave, index)
  end

private

  def slave_status(index=1)
    status(:slave, index).values_at(9, 21)
  end

  def master_status
    status(:master).values_at(0, 1)
  end

  def status(name, index=1)
    `mysql --protocol=TCP -P#{port(name, index)} -uroot -N -s -e 'show #{name} status'`.strip.split("\t")
  end

  def execute(statement, name, index=1)
    system(%{mysql --protocol=TCP -P#{port(name, index)} -uroot -e "#{statement}"})
  end

  def start(name, index=1)
    return if started?(name, index)

    $forks ||= {}
    $forks[[name, index]] = fork do
      exec("mysqld --defaults-file='#{location(name, index)}/my.cnf'")
    end

    wait_for_database_boot(name, index)
  end

  def stop(name, index=1)
    if fork = $forks.delete([name, index])
      Process.kill("KILL", fork)
      Process.wait(fork)
    end
  end

  def started?(host, index=1)
    system(%{mysql --protocol=TCP -P#{port(host)} -uroot -e '' 2> /dev/null})
  end

  def wait_for_database_boot(host, index=1)
    Timeout.timeout(10) do
      until started?(host, index); sleep(0.1); end
    end
  rescue Timeout::Error
    raise "Couldn't connect to MySQL in time"
  end

  def location(name, index=1)
    File.expand_path(
      File.join("..", "mysql", name.to_s, index.to_s),
      File.dirname(__FILE__))
  end

  def config(name, index=1)
    path = location(name, index)

    <<-EOS
[mysqld]
pid-file                = #{path}/mysqld.pid
socket                  = #{path}/mysqld.sock
port                    = #{port(name)}
log-error               = #{path}/error.log
datadir                 = #{path}/data
log-bin                 = #{name}-bin
log-bin-index           = #{name}-bin.index
server-id               = #{server_id(name, index)}
lower_case_table_names  = 1
sql-mode                = ''
replicate-ignore-db     = mysql
    EOS
  end
end
