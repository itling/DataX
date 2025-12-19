package com.alibaba.datax.plugin.writer.tdengine30writer;

import com.alibaba.datax.common.util.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SchemaCacheTest {

    private String config;

    @Test
    @Ignore
    public void testSchemaCache() {
   
                Configuration config = Configuration.from(this.config);
                SchemaCache schemaCache = SchemaCache.getInstance(config);

                List<ColumnMeta> col_metas = schemaCache.getColumnMetaList("gnss", TableType.SUP_TABLE);
                Assert.assertEquals(10, col_metas.size());

        
    }

    @Before
    public void before() {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("gnss.json");
        try {
            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            this.config = new String(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}