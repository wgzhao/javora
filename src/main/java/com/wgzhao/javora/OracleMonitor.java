package com.wgzhao.javora;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.simple.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

public class OracleMonitor
{

    private static final Map<String, String> oracleMonitorSql = new HashMap<>();

    private static Boolean isVerbose = false;

    static {
        // count of active users
        oracleMonitorSql.put("active_user", "select to_char(count(*)-1, 'FM99999999999999990') retvalue " +
                "              from v$session where username is not null " +
                "              and status='ACTIVE'");

        // check instance is alive and open
        oracleMonitorSql.put("check_active", "select to_char(case when inst_cnt > 0 then 1 else 0 end, " +
                "              'FM99999999999999990') retvalue from (select count(*) inst_cnt " +
                "              from v$instance where status = 'OPEN' and logins = 'ALLOWED' " +
                "              and database_status = 'ACTIVE')");

        // Read cache hit ratio
        oracleMonitorSql.put("rcachehit", "SELECT nvl(to_char((1 - (phy.value - lob.value - dir.value) / " +
                "              ses.value) * 100, 'FM99999990.9999'), '0') retvalue " +
                "              FROM   v$sysstat ses, v$sysstat lob, " +
                "              v$sysstat dir, v$sysstat phy " +
                "              WHERE  ses.name = 'session logical reads' " +
                "              AND    dir.name = 'physical reads direct' " +
                "              AND    lob.name = 'physical reads direct (lob)' " +
                "              AND    phy.name = 'physical reads'");

        // Disk sort ratio
        oracleMonitorSql.put("dsksortratio", "SELECT nvl(to_char(d.value/(d.value + m.value)*100, " +
                "              'FM99999990.9999'), '0') retvalue " +
                "              FROM  v$sysstat m, v$sysstat d " +
                "              WHERE m.name = 'sorts (memory)' " +
                "              AND d.name = 'sorts (disk)'");
        // size of user data ( without temp)
        oracleMonitorSql.put("dbsize", "SELECT to_char(sum(  NVL(a.bytes - NVL(f.bytes, 0), 0)), " +
                "              'FM99999999999999990') retvalue " +
                "              FROM sys.dba_tablespaces d, " +
                "              (select tablespace_name, sum(bytes) bytes from dba_data_files " +
                "              group by tablespace_name) a, " +
                "              (select tablespace_name, sum(bytes) bytes from " +
                "              dba_free_space group by tablespace_name) f " +
                "              WHERE d.tablespace_name = a.tablespace_name(+) AND " +
                "              d.tablespace_name = f.tablespace_name(+) " +
                "              AND NOT (d.extent_management like 'LOCAL' AND d.contents " +
                "              like 'TEMPORARY')");

        // size of all datafiles
        oracleMonitorSql.put("dbfilesize", "select to_char(sum(bytes), 'FM99999999999999990') retvalue " +
                "              from dba_data_files");

        // oracle version (banner)
        oracleMonitorSql.put("version", "select banner from v$version where rownum=1");

        // Instance uptime (seconds)
        oracleMonitorSql.put("uptime", "select to_char((sysdate-startup_time)*86400, " +
                "              'FM99999999999999990') retvalue from v$instance");

        // User commits
        oracleMonitorSql.put("commits", "select nvl(to_char(value, 'FM99999999999999990'), '0') retvalue from " +
                "              v$sysstat where name = 'user commits'");

        // User rollbacks
        oracleMonitorSql.put("rollbacks", "select nvl(to_char(value, 'FM99999999999999990'), '0') retvalue from " +
                "v$sysstat where name = 'user rollbacks'");

        // Deadlocks
        oracleMonitorSql.put("deadlocks", "select nvl(to_char(value, 'FM99999999999999990'), '0') retvalue from " +
                "              v$sysstat where name = 'enqueue deadlocks'");

        // Redo writes
        oracleMonitorSql.put("redowrites", "select nvl(to_char(value, 'FM99999999999999990'), '0') retvalue from " +
                "              v$sysstat where name = 'redo writes'");

        // Table scans (long tables)
        oracleMonitorSql.put("tblscans", "select nvl(to_char(value, 'FM99999999999999990'), '0') retvalue from " +
                "              v$sysstat where name = 'table scans (long tables)'");

        // Table scan rows gotten
        oracleMonitorSql.put("tblrowsscans", "select nvl(to_char(value, 'FM99999999999999990'), '0') retvalue from " +
                "              v$sysstat where name = 'table scan rows gotten'");

        // Index fast full scans (full)
        oracleMonitorSql.put("indexffs", "select nvl(to_char(value, 'FM99999999999999990'), '0') retvalue from " +
                "              v$sysstat where name = 'index fast full scans (full)'");

        // Hard parse ratio
        oracleMonitorSql.put("hparsratio", "SELECT nvl(to_char(h.value/t.value*100,'FM99999990.9999'), '0') " +
                "              retvalue FROM  v$sysstat h, v$sysstat t WHERE h.name = 'parse " +
                "              count (hard)' AND t.name = 'parse count (total)'");

        // Bytes sent via SQL*Net to client
        oracleMonitorSql.put("netsent", "select nvl(to_char(value, 'FM99999999999999990'), '0') retvalue from " +
                "              v$sysstat where name = 'bytes sent via SQL*Net to client'");

        // Bytes received SQL*Net from client
        oracleMonitorSql.put("netresv", "select nvl(to_char(value, 'FM99999999999999990'), '0') retvalue from " +
                "              v$sysstat where name = 'bytes received via SQL*Net from client'");

        // SQL*Net roundtrips to/from client

        oracleMonitorSql.put("netroundtrips", "select nvl(to_char(value, 'FM99999999999999990'), '0') retvalue from " +
                "              v$sysstat where name = 'SQL*Net roundtrips to/from client'");

        // Logons current
        oracleMonitorSql.put("logonscurrent", "select nvl(to_char(value, 'FM99999999999999990'), '0') retvalue from " +
                "              v$sysstat where name = 'logons current'");

        // Last archived log sequence
        oracleMonitorSql.put("lastarclog", "select to_char(max(SEQUENCE#), 'FM99999999999999990') " +
                "              retvalue from v$log where archived = 'YES'");

        // Last applied archive log (at standby).Next items requires [timed_statistics = true]
        oracleMonitorSql.put("lastapplarclog", "select to_char(max(lh.SEQUENCE#), 'FM99999999999999990') " +
                "              retvalue from v$loghist lh, v$archived_log al " +
                "              where lh.SEQUENCE# = al.SEQUENCE# and applied='YES'");

        // Free buffer waits
        oracleMonitorSql.put("freebufwaits", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en " +
                "              where se.event(+) = en.name and en.name = 'free buffer waits'");

        // Buffer busy waits
        oracleMonitorSql.put("bufbusywaits", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en where se.event(+) = " +
                "              en.name and en.name = 'buffer busy waits'");

        // Log file switch completion
        oracleMonitorSql.put("logswcompletion", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en where se.event(+) " +
                "              = en.name and en.name = 'log file switch completion'");

        // Log file sync
        oracleMonitorSql.put("logfilesync", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en " +
                "              where se.event(+) = en.name and en.name = 'log file sync'");

        // Log file parallel write
        oracleMonitorSql.put("logprllwrite", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en where se.event(+) " +
                "              = en.name and en.name = 'log file parallel write'");

        // Enqueue waits
        oracleMonitorSql.put("enqueue", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en " +
                "              where se.event(+) = en.name and en.name = 'enqueue'");

        // DB file sequential read waits
        oracleMonitorSql.put("dbseqread", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en where se.event(+) " +
                "              = en.name and en.name = 'db file sequential read'");

        // DB file scattered read
        oracleMonitorSql.put("dbscattread", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en where se.event(+) " +
                "              = en.name and en.name = 'db file scattered read'");

        // DB file single write
        oracleMonitorSql.put("dbsnglwrite", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en where se.event(+) " +
                "              = en.name and en.name = 'db file single write'");

        // DB file parallel write waits
        oracleMonitorSql.put("dbprllwrite", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en where se.event(+) " +
                "              = en.name and en.name = 'db file parallel write'");

        // Direct path read
        oracleMonitorSql.put("directread", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en where se.event(+) " +
                "              = en.name and en.name = 'direct path read'");

        // Direct path write
        oracleMonitorSql.put("directwrite", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en where se.event(+) " +
                "              = en.name and en.name = 'direct path write'");

        // Latch free
        oracleMonitorSql.put("latchfree", "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue " +
                "              from v$system_event se, v$event_name en where se.event(+) " +
                "              = en.name and en.name = 'latch free'");

        // List tablespace names in a JSON like format for Zabbix use
        // output:
        // {
        //     "data":[
        //     { "{#TABLESPACE}":"ORASDPM"},
        //     { "{#TABLESPACE}":"MDS"},
        //     { "{#TABLESPACE}":"SOADEV_MDS"},
        //     { "{#TABLESPACE}":"ORABAM"},
        //     { "{#TABLESPACE}":"SOAINF"},
        //     { "{#TABLESPACE}":"DATA"},
        //     { "{#TABLESPACE}":"MGMT_AD4J_TS"},
        //     { "{#TABLESPACE}":"MGMT_ECM_DEPOT_TS"},
        //     { "{#TABLESPACE}":"MGMT_TABLESPACE"},
        //     { "{#TABLESPACE}":"RECOVER"},
        //     { "{#TABLESPACE}":"RMAN_CAT"},
        //     { "{#TABLESPACE}":"SYSAUX"},
        //     { "{#TABLESPACE}":"SYSTEM"},
        //     { "{#TABLESPACE}":"TEMP"},
        //     { "{#TABLESPACE}":"UNDOTBS"},
        //     { "{#TABLESPACE}":"VIRTUALCENTER"},
        //     ]
        // }
        oracleMonitorSql.put("show_tablespaces", "SELECT tablespace_name FROM dba_tablespaces order by 1");

        // List temporary tablespace names in a JSON like format for Zabbix use
        oracleMonitorSql.put("show_tablespace_temp", "SELECT TABLESPACE_NAME FROM DBA_TABLESPACES WHERE CONTENTS='TEMPORARY'");

        // List als ASM volumes in a JSON like format for Zabbix use
        oracleMonitorSql.put("show_asm_volumes", "select NAME from v$asm_diskgroup_stat ORDER BY 1");

        // query lock
        oracleMonitorSql.put("query_lock", "SELECT count(*) FROM gv$lock l WHERE  block=1");

        // Query redo logs
        oracleMonitorSql.put("query_redologs", "select COUNT(*) from v$LOG WHERE STATUS='ACTIVE'");

        // Query rollback
        oracleMonitorSql.put("query_rollbacks", "select nvl(trunc(sum(used_ublk*4096)/1024/1024),0) from " +
                "              gv$transaction t,gv$session s where ses_addr = saddr");

        // Query session
        oracleMonitorSql.put("query_sessions", "select count(*) from gv$session where username is not null and status='ACTIVE'");

        // Query the Fast Recovery Area usage
        oracleMonitorSql.put("fra_use", "select round((SPACE_LIMIT-(SPACE_LIMIT-SPACE_USED))/SPACE_LIMIT*100,2) FROM V$RECOVERY_FILE_DEST");

        // Query the list of users on the instance
        oracleMonitorSql.put("show_users", "SELECT username FROM dba_users ORDER BY 1");

        // Get table space in usage
        oracleMonitorSql.put("tablespace", "SELECT 100-(TRUNC((max_free_mb/max_size_mb) * 100)) AS USED " +
                "  FROM ( SELECT a.tablespace_name,b.size_mb,a.free_mb,b.max_size_mb,a.free_mb + (b.max_size_mb - b.size_mb) AS max_free_mb" +
                "  FROM   (SELECT tablespace_name,TRUNC(SUM(bytes)/1024/1024) AS free_mb FROM dba_free_space GROUP BY tablespace_name) a," +
                "  (SELECT tablespace_name,TRUNC(SUM(bytes)/1024/1024) AS size_mb,TRUNC(SUM(GREATEST(bytes,maxbytes))/1024/1024) AS max_size_mb" +
                "  FROM   dba_data_files GROUP BY tablespace_name) b WHERE  a.tablespace_name = b.tablespace_name" +
                "  ) where tablespace_name='%s'");

        // Get tablespace in use, pass tablespace name
        oracleMonitorSql.put("tablespace_abs", "SELECT (df.totalspace -  tu.totalusedspace) AS FREEMB from (select tablespace_name, " +
                "  sum(bytes) TotalSpace from dba_data_files group by tablespace_name)  df ," +
                " (select sum(bytes) totalusedspace,tablespace_name from dba_segments " +
                "  group by tablespace_name) tu WHERE tu.tablespace_name = df.tablespace_name and df.tablespace_name = '%s' ");

        // List archive used
        oracleMonitorSql.put("check_archive", "select trunc((total_mb-free_mb)*100/(total_mb)) PCT from v$asm_diskgroup_stat where name='%s'");

        // Query temporary tablespaces, pass tablespace name
        oracleMonitorSql.put("tablespace_temp", "SELECT round(((TABLESPACE_SIZE-FREE_SPACE)/TABLESPACE_SIZE)*100,2) " +
                "              PERCENTUAL FROM dba_temp_free_space where  tablespace_name='%s'");

        // Query v$sysmetric parameters, pass metric name
        oracleMonitorSql.put("query_sysmetrics", "select value from v$sysmetric where METRIC_NAME ='{name.replace(\"_\", \" \")}' and rownum <=1");

        // Get ASM volume usage
        oracleMonitorSql.put("asm_volume_use", "select round(((TOTAL_MB-FREE_MB)/TOTAL_MB*100),2) from " +
                "              v$asm_diskgroup_stat where name = '%s'");

        // Determines whether a user is locked or not, pass username
        oracleMonitorSql.put("user_status", "SELECT account_status FROM dba_users WHERE username='%s'");
    }

    private static final List<String> needParams = Arrays.asList("tablespace", "check_archive", "tablespace_temp", "tablespace_abs",
            "user_status", "asm_volume_use");
    private static final List<String> needJson = Arrays.asList("show_tablespaces", "show_tablespace_temp", "show_asm_volumes");

    private static void oracleMonitor(String url, String username, String password, String kind, String... param)
    {
        Connection connection;
        Statement statement;

        try {
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement();

            if (oracleMonitorSql.containsKey(kind)) {
                String sql = oracleMonitorSql.get(kind);
                // some sql need argument
                if (needParams.contains(kind)) {
                    sql = String.format(sql, param[0]);
                }
                if (isVerbose) {
                    System.out.println("query sql is : " + sql);
                }
                ResultSet resultSet = statement.executeQuery(sql);
                if (needJson.contains(kind)) {
                    JSONObject object = new JSONObject();

                    List<Map<String, String>> res = new ArrayList<>();
                    while (resultSet.next()) {
                        Map<String, String> map = new HashMap<>();
                        map.put("{#TABLESPACE}", resultSet.getString(1));
                        res.add(map);
                    }
                    object.put("data", res);
                    System.out.println(object.toJSONString());
                }
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
            else {
                System.out.println("0");
                System.out.println("no such kind: " + kind);
                StringJoiner stringJoiner = new StringJoiner(" ");
                for (String k : oracleMonitorSql.keySet()) {
                    stringJoiner.add(k);
                }
                System.out.println("available kind is: " + stringJoiner);
            }

            statement.close();
            connection.close();
        }
        catch (SQLException throwables) {
            System.out.println("0");
            System.out.println(throwables.getMessage());
        }
    }

    public static void main(String[] args)
    {

        // default value
        String username = "zabbix";
        String password = "password";
        String host = "localhost";
        String port = "1521";
        String database = "orcl";
        String kind = "check_active";

        Options options = new Options();
        options.addOption(new Option("u", "username", true, "username to connect"));
        options.addOption(new Option("p", "password", true, "the password for username"));
        options.addOption(new Option("h", "host", true, "host to connect"));
        options.addOption(new Option("P", "port", true, "listen port"));
        options.addOption(new Option("d", "database", true, "oracle SID or database"));
        options.addOption(new Option("k", "kind", true, "which check you want to do"));
        Option opt = new Option("v", "verbose", false, "more output for debug");
        opt.setRequired(false);
        options.addOption(opt);
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e) {
            System.out.println("0");
            System.out.println(e.getMessage());
        }
        assert cmd != null;
        if (cmd.hasOption("u")) {
            username = cmd.getOptionValue("u");
        }
        if (cmd.hasOption("p")) {
            password = cmd.getOptionValue("p");
        }
        if (cmd.hasOption("h")) {
            host = cmd.getOptionValue("h");
        }
        if (cmd.hasOption("P")) {
            port = cmd.getOptionValue("port");
        }
        if (cmd.hasOption("d")) {
            database = cmd.getOptionValue("d");
        }
        if (cmd.hasOption("k")) {
            kind = cmd.getOptionValue("k");
        }

        if (cmd.hasOption("v")) {
            isVerbose = true;
        }

        String url = "jdbc:oracle:thin:@" + host + ":" + port + "/" + database;
        if (needParams.contains(kind)) {
            if (cmd.getArgs().length < 1) {
                System.out.println("0");
                System.out.println("the kind " + kind + " need a argument");
                return;
            }
            oracleMonitor(url, username, password, kind, cmd.getArgs()[0]);
        }
        else {
            oracleMonitor(url, username, password, kind);
        }
    }
}
