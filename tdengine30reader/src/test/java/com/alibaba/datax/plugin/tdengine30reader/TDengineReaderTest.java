package com.alibaba.datax.plugin.tdengine30reader;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.tdengine30reader.TDengineReader;
import com.alibaba.datax.plugin.writer.tdengine30writer.Key;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TDengineReaderTest {

    @Test
    public void jobInit_case01() {
        // given
        TDengineReader.Job job = new TDengineReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"connection\": [{\"table\":[\"weather\"],\"jdbcUrl\":[\"jdbc:TAOS-RS://master:6041/test\"]}]," +
                "\"column\": [\"ts\",\"current\",\"voltage\",\"phase\"]," +
                "\"where\":\"_c0 > 0\"," +
                "\"beginDateTime\": \"2021-01-01 00:00:00\"," +
                "\"endDateTime\": \"2021-01-01 12:00:00\"" +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();

        // assert
        Configuration conf = job.getPluginJobConf();

        Assert.assertEquals("root", conf.getString(Key.USERNAME));
        Assert.assertEquals("taosdata", conf.getString("password"));
        Assert.assertEquals("weather", conf.getString("connection[0].table[0]"));
        Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("connection[0].jdbcUrl[0]"));
        Assert.assertEquals("2021-01-01 00:00:00", conf.getString("beginDateTime"));
        Assert.assertEquals("2021-01-01 12:00:00", conf.getString("endDateTime"));
        Assert.assertEquals("_c0 > 0", conf.getString("where"));
    }


    @Test
    public void jobInit_case02() {
        // given
        TDengineReader.Job job = new TDengineReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"connection\": [{\"querySql\":[\"select * from weather\"],\"jdbcUrl\":[\"jdbc:TAOS-RS://master:6041/test\"]}]," +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();

        // assert
        Configuration conf = job.getPluginJobConf();

        Assert.assertEquals("root", conf.getString(Key.USERNAME));
        Assert.assertEquals("taosdata", conf.getString("password"));
        Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("connection[0].jdbcUrl[0]"));
        Assert.assertEquals("select * from weather", conf.getString("connection[0].querySql[0]"));
    }

    @Test
    public void jobSplit_case01() {
        // given
        TDengineReader.Job job = new TDengineReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"connection\": [{\"table\":[\"weather\"],\"jdbcUrl\":[\"jdbc:TAOS-RS://master:6041/test\"]}]," +
                "\"column\": [\"ts\",\"current\",\"voltage\",\"phase\"]," +
                "\"where\":\"_c0 > 0\"," +
                "\"beginDateTime\": \"2021-01-01 00:00:00\"," +
                "\"endDateTime\": \"2021-01-01 12:00:00\"" +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();
        List<Configuration> configurationList = job.split(1);

        // assert
        Assert.assertEquals(1, configurationList.size());
        Configuration conf = configurationList.get(0);
        Assert.assertEquals("root", conf.getString("username"));
        Assert.assertEquals("taosdata", conf.getString("password"));
        Assert.assertEquals("_c0 > 0", conf.getString("where"));
        Assert.assertEquals("weather", conf.getString("table[0]"));
        Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("jdbcUrl"));

    }

    @Test
    public void jobSplit_case02() {
        // given
        TDengineReader.Job job = new TDengineReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"connection\": [{\"querySql\":[\"select * from weather\"],\"jdbcUrl\":[\"jdbc:TAOS-RS://master:6041/test\"]}]," +
                "\"column\": [\"ts\",\"current\",\"voltage\",\"phase\"]," +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();
        List<Configuration> configurationList = job.split(1);

        // assert
        Assert.assertEquals(1, configurationList.size());
        Configuration conf = configurationList.get(0);
        Assert.assertEquals("root", conf.getString("username"));
        Assert.assertEquals("taosdata", conf.getString("password"));
        Assert.assertEquals("select * from weather", conf.getString("querySql[0]"));
        Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("jdbcUrl"));
    }

    @Test
    public void jobSplit_case03() {
        // given
        TDengineReader.Job job = new TDengineReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"connection\": [{\"querySql\":[\"select * from weather\",\"select * from test.meters\"],\"jdbcUrl\":[\"jdbc:TAOS-RS://master:6041/test\", \"jdbc:TAOS://master:6030/test\"]}]," +
                "\"column\": [\"ts\",\"current\",\"voltage\",\"phase\"]," +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();
        List<Configuration> configurationList = job.split(1);

        // assert
        Assert.assertEquals(2, configurationList.size());
        Configuration conf = configurationList.get(0);
        Assert.assertEquals("root", conf.getString("username"));
        Assert.assertEquals("taosdata", conf.getString("password"));
        Assert.assertEquals("select * from weather", conf.getString("querySql[0]"));
        Assert.assertEquals("jdbc:TAOS-RS://master:6041/test", conf.getString("jdbcUrl"));

        Configuration conf1 = configurationList.get(1);
        Assert.assertEquals("root", conf1.getString("username"));
        Assert.assertEquals("taosdata", conf1.getString("password"));
        Assert.assertEquals("select * from weather", conf1.getString("querySql[0]"));
        Assert.assertEquals("select * from test.meters", conf1.getString("querySql[1]"));
        Assert.assertEquals("jdbc:TAOS://master:6030/test", conf1.getString("jdbcUrl"));
    }

    @Test
    public void taskInit_splitSubtable_case01() {
        // given
        TDengineReader.Task task = new TDengineReader.Task();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"jdbcUrl\": \"jdbc:TAOS-RS://master:6041/vehicle_dev\"," +
                "\"table\": [\"pvt\"]," +
                "\"column\": [\"ts\",\"temperature\",\"pressure\",\"speed\"]," +
                "\"where\":\"_c0 > 0\"," +
                "\"beginDateTime\": \"2025-09-01 00:00:00\"," +
                "\"endDateTime\": \"2025-10-01 00:00:00\"," +
                "\"splitSubtable\": 500" +
                "}");
        task.setPluginJobConf(configuration);

        // when
        task.init();

        // assert
        Configuration conf = task.getPluginJobConf();
        Assert.assertEquals(500, conf.getInt("splitSubtable", 0).intValue());

    }

    @Test
    public void taskInit_splitSubtable_case02() {
        // given
        TDengineReader.Task task = new TDengineReader.Task();
        Configuration configuration = Configuration.from("{" +
                 "\"username\": \"root\"," +
                 "\"password\": \"taosdata\"," +
                 "\"jdbcUrl\": \"jdbc:TAOS-RS://master:6041/vehicle_dev\"," +
                 "\"table\": [\"pvt\"]," +
                 "\"column\": [\"ts\",\"temperature\",\"pressure\",\"speed\"]," +
                 "\"where\":\"_c0 > 0\"" +
                 "}");
        task.setPluginJobConf(configuration);

        // when
        task.init();

        // assert
        Configuration conf = task.getPluginJobConf();
        // 默认值应为0
        Assert.assertEquals(0, conf.getInt("splitSubtable", 0).intValue());
    }

}