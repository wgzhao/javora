package com.wgzhao.javora;

import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@ShellComponent
public class OracleMonitor
{

    Map<String, String> kindSql = new HashMap<>();

    {
        // count of active users
        kindSql.put("active_user", "select to_char(count(*)-1, 'FM99999999999999990') retvalue " +
                "              from v$session where username is not null " +
                "              and status='ACTIVE'");

        // check instance is alive and open
        kindSql.put("check_alive", "select to_char(case when inst_cnt > 0 then 1 else 0 end, " +
                "              'FM99999999999999990') retvalue from (select count(*) inst_cnt " +
                "              from v$instance where status = 'OPEN' and logins = 'ALLOWED' " +
                "              and database_status = 'ACTIVE')");

        // Read cache hit ratio
        kindSql.put("rcachehit", "SELECT nvl(to_char((1 - (phy.value - lob.value - dir.value) / \\\n" +
                "              ses.value) * 100, 'FM99999990.9999'), '0') retvalue \\\n" +
                "              FROM   v$sysstat ses, v$sysstat lob, \\\n" +
                "              v$sysstat dir, v$sysstat phy \\\n" +
                "              WHERE  ses.name = 'session logical reads' \\\n" +
                "              AND    dir.name = 'physical reads direct' \\\n" +
                "              AND    lob.name = 'physical reads direct (lob)' \\\n" +
                "              AND    phy.name = 'physical reads'");

        // Disk sort ratio
        kindSql.put("dsksortratio", "SELECT nvl(to_char(d.value/(d.value + m.value)*100, \\\n" +
                "              'FM99999990.9999'), '0') retvalue \\\n" +
                "              FROM  v$sysstat m, v$sysstat d \\\n" +
                "              WHERE m.name = 'sorts (memory)' \\\n" +
                "              AND d.name = 'sorts (disk)'");
    }

    @ShellMethod("Monitor Oracle status")
    public String monitor(@ShellOption(value = "-u", defaultValue = "zabbix") String username,
            @ShellOption(value = "-p", defaultValue = "zabbix") String password,
            @ShellOption(value = "-h", defaultValue = "localhost") String address,
            @ShellOption(value = "-d", defaultValue = "orcl") String database,
            @ShellOption(value = "-P", defaultValue = "1521") int port,
            String kind)
            throws SQLException
    {
        String url = "jdbc:oracle:thin:@" + address + ":" + port + "/" + database;
        Connection connection = DriverManager.getConnection(url, username, password);
        Statement statement = connection.createStatement();
        if (kindSql.containsKey(kind)) {
            ResultSet resultSet = statement.executeQuery(kindSql.get(kind));
            if (resultSet.next()) {
                return resultSet.getString(1);
            }
        }
        else {
            return "no such kind";
        }
        return "Error";
    }
}
