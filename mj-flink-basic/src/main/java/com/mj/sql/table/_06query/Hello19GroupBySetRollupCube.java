package com.mj.sql.table._06query;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class Hello19GroupBySetRollupCube {

    public static void main(String[] args) {
        // 批处理运行环境
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tableEnvironment.sqlQuery("""
                SELECT pid, sum(num) AS total
                FROM (VALUES
                ('省1','市1','县1',100),
                ('省1','市2','县2',101),
                ('省1','市2','县1',102),
                ('省2','市1','县4',103),
                ('省2','市2','县1',104),
                ('省2','市2','县1',105),
                ('省3','市1','县1',106),
                ('省3','市2','县1',107),
                ('省3','市2','县2',108),
                ('省4','市1','县1',109),
                ('省4','市2','县1',110))
                AS t_person_num(pid, cid, xid, num)
                GROUP BY pid HAVING sum(num) > 300
                """).execute().print();

        tableEnvironment.sqlQuery("""
                SELECT pid, cid, xid, sum(num) AS total,
                GROUPING(pid) AS g_pid,
                GROUPING(cid) AS g_cid,
                GROUPING(xid) AS g_xid
                FROM (VALUES
                ('省1','市1','县1',100),
                ('省1','市2','县2',101),
                ('省1','市2','县1',102),
                ('省2','市1','县4',103),
                ('省2','市2','县1',104),
                ('省2','市2','县1',105),
                ('省3','市1','县1',106),
                ('省3','市2','县1',107),
                ('省3','市2','县2',108),
                ('省4','市1','县1',109),
                ('省4','市2','县1',110))
                AS t_person_num(pid, cid, xid, num)
                GROUP BY GROUPING SETS ((pid, cid, xid), (pid, cid), (pid), ())
                ORDER BY g_pid desc, g_cid desc, g_xid desc, pid, cid, xid
                """).execute().print();
        // roleup 等于上面所有的grouping sets
        tableEnvironment.sqlQuery("""
                SELECT pid, cid, xid, sum(num) AS total,
                GROUPING(pid) AS g_pid,
                GROUPING(cid) AS g_cid,
                GROUPING(xid) AS g_xid
                FROM (VALUES
                ('省1','市1','县1',100),
                ('省1','市2','县2',101),
                ('省1','市2','县1',102),
                ('省2','市1','县4',103),
                ('省2','市2','县1',104),
                ('省2','市2','县1',105),
                ('省3','市1','县1',106),
                ('省3','市2','县1',107),
                ('省3','市2','县2',108),
                ('省4','市1','县1',109),
                ('省4','市2','县1',110))
                AS t_person_num(pid, cid, xid, num)
                GROUP BY ROLLUP (pid, cid, xid)
                ORDER BY g_pid desc, g_cid desc, g_xid desc, pid, cid, xid
                """).execute().print();

        tableEnvironment.sqlQuery("""
                SELECT pid, cid, xid, sum(num) AS total,
                GROUPING(pid) AS g_pid,
                GROUPING(cid) AS g_cid,
                GROUPING(xid) AS g_xid
                FROM (VALUES
                ('省1','市1','县1',100),
                ('省1','市2','县2',101),
                ('省1','市2','县1',102),
                ('省2','市1','县4',103),
                ('省2','市2','县1',104),
                ('省2','市2','县1',105),
                ('省3','市1','县1',106),
                ('省3','市2','县1',107),
                ('省3','市2','县2',108),
                ('省4','市1','县1',109),
                ('省4','市2','县1',110))
                AS t_person_num(pid, cid, xid, num)
                GROUP BY ROLLUP (pid, cid, xid)
                ORDER BY g_pid desc, g_cid desc, g_xid desc, pid, cid, xid
                """).execute().print();

        tableEnvironment.sqlQuery("""
                SELECT pid, cid, xid, sum(num) AS total,
                GROUPING(pid) AS g_pid,
                GROUPING(cid) AS g_cid,
                GROUPING(xid) AS g_xid
                FROM (VALUES
                ('省1','市1','县1',100),
                ('省1','市2','县2',101),
                ('省1','市2','县1',102),
                ('省2','市1','县4',103),
                ('省2','市2','县1',104),
                ('省2','市2','县1',105),
                ('省3','市1','县1',106),
                ('省3','市2','县1',107),
                ('省3','市2','县2',108),
                ('省4','市1','县1',109),
                ('省4','市2','县1',110))
                AS t_person_num(pid, cid, xid, num)
                GROUP BY CUBE (pid, cid, xid)
                ORDER BY g_pid desc, g_cid desc, g_xid desc, pid, cid, xid
                """).execute().print();

    }

}
