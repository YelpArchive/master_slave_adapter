require 'fileutils'
require 'timeout'

module MysqlSetupHelper
  TEST_TABLE  = "master_slave_adapter.master_slave_test"

  def master_id(*args); "1"; end
  def slave_id(index); (master_id.to_i + index).to_s; end

  def master_port(*args); 3310; end
  def slave_port(index); master_port + index; end

  def port(name, index)
    send("#{name}_port", index)
  end

  def server_id(name, index)
    send("#{name}_id", index)
  end

  def start_replication(index)
    execute("start slave", :slave, index)
  end

  def stop_replication(index)
    execute("stop slave", :slave, index)
  end

  def move_master_clock
    execute("insert into #{TEST_TABLE} (message) VALUES ('test')", :master, 1)
  end

  def wait_for_replication_sync(index)
    Timeout.timeout(5) do
      until slave_status(index) == master_status; end
    end
  rescue Timeout::Error
    raise "Replication synchronization failed"
  end

  def configure_master(*args)
    execute(<<-EOS, :master, 1)
      SET sql_log_bin = 0;
      create user 'slave'@'localhost' identified by 'slave';
      grant replication slave on *.* to 'slave'@'localhost';
      create database master_slave_adapter;
      SET sql_log_bin = 1;
    EOS

    execute(<<-EOS, :master, 1)
      CREATE TABLE #{TEST_TABLE} (
        id int(11) NOT NULL AUTO_INCREMENT,
        message text COLLATE utf8_unicode_ci,
        created_at datetime DEFAULT NULL,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
    EOS
  end

  def configure_slave(index)
    execute(<<-EOS, :slave, index)
      change master to master_user = 'slave',
             master_password = 'slave',
             master_port = #{port(:master, 1)},
             master_host = 'localhost';
      create database master_slave_adapter;
    EOS
  end

  def setup_for(name, index)
    path        = location(name, index)
    config_path = File.join(path, "my.cnf")
    base_dir    = File.dirname(File.dirname(`which mysql_install_db`))

    FileUtils.rm_rf(path)
    FileUtils.mkdir_p(path)
    File.open(config_path, "w") { |file| file << config(name, index) }

    `mysql_install_db --defaults-file='#{config_path}' --basedir='#{base_dir}' --user=''`
  end

  def setup
    setup_for(:master, 1)
    setup_for(:slave, 1)
    setup_for(:slave, 2)
  end

  def start_master
    start(:master, 1)
  end

  def stop_master
    stop(:master, 1)
  end

  def start_slave(index)
    start(:slave, index)
  end

  def stop_slave(index)
    stop(:slave, index)
  end

private

  def slave_status(index)
    status(:slave, index).values_at(9, 21)
  end

  def master_status
    status(:master, 1).values_at(0, 1)
  end

  def status(name, index)
    `mysql --protocol=TCP -P#{port(name, index)} -uroot -N -s -e 'show #{name} status'`.strip.split("\t")
  end

  def execute(statement, name, index)
    system(%{mysql --protocol=TCP -P#{port(name, index)} -uroot -e "#{statement}"})
  end

  def start(name, index)
    return if started?(name, index)

    $forks ||= {}
    $forks[[name, index]] = fork do
      exec("mysqld --defaults-file='#{location(name, index)}/my.cnf'")
    end

    wait_for_database_boot(name, index)
  end

  def stop(name, index)
    if fork = $forks.delete([name, index])
      Process.kill("KILL", fork)
      Process.wait(fork)
    end
  end

  def started?(name, index)
    system(%{mysql --protocol=TCP -P#{port(name, index)} -uroot -e '' 2> /dev/null})
  end

  def wait_for_database_boot(name, index)
    Timeout.timeout(10) do
      until started?(name, index); sleep(0.1); end
    end
  rescue Timeout::Error
    raise "Couldn't connect to MySQL in time"
  end

  def location(name, index)
    File.expand_path(
      File.join("..", "mysql", "#{name}_#{index}"),
      File.dirname(__FILE__))
  end

  def config(name, index)
    path = location(name, index)

    <<-EOS
[mysqld]
pid-file                = #{path}/mysqld.pid
socket                  = #{path}/mysqld.sock
port                    = #{port(name, index)}
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
