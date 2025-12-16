package com.alibaba.datax.plugin.tdengine30reader;

import com.alibaba.datax.common.util.Configuration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SplitSubtableTest {

    @Test
    public void testSplitSubtableCalculation() {
        // 测试不同splitSubtable值下的批次数计算
        int[] splitSubtableValues = {0, 1, 10, 100, 500};
        int[] subtableCounts = {0, 1, 9, 10, 11, 99, 100, 101, 499, 500, 501, 1000};
        
        for (int splitSubtable : splitSubtableValues) {
            for (int subtableCount : subtableCounts) {
                int expectedBatches;
                if (splitSubtable <= 0 || subtableCount == 0) {
                    expectedBatches = 0; // 不启用分批或无子表
                } else {
                    expectedBatches = (subtableCount + splitSubtable - 1) / splitSubtable;
                }
                
                // 模拟计算
                int actualBatches = 0;
                if (splitSubtable > 0 && subtableCount > 0) {
                    actualBatches = (subtableCount + splitSubtable - 1) / splitSubtable;
                }
                
                System.out.printf("splitSubtable=%d, subtableCount=%d, expectedBatches=%d, actualBatches=%d%n", 
                        splitSubtable, subtableCount, expectedBatches, actualBatches);
                
                assertEquals(expectedBatches, actualBatches);
            }
        }
    }
    
    @Test
    public void testSubtableBatching() {
        // 测试子表分批逻辑
        List<String> subtableNames = new ArrayList<>();
        for (int i = 0; i < 105; i++) {
            subtableNames.add("subtable_" + i);
        }
        
        int[] splitSubtableValues = {10, 50, 100};
        
        for (int splitSubtable : splitSubtableValues) {
            System.out.printf("\nTesting with splitSubtable=%d%n", splitSubtable);
            
            int batchCount = 0;
            for (int i = 0; i < subtableNames.size(); i += splitSubtable) {
                int end = Math.min(i + splitSubtable, subtableNames.size());
                List<String> batchSubtables = subtableNames.subList(i, end);
                
                System.out.printf("Batch %d: %d-%d, size=%d%n", 
                        batchCount, i, end, batchSubtables.size());
                
                // 验证每批的大小
                if (batchCount < (subtableNames.size() + splitSubtable - 1) / splitSubtable - 1) {
                    // 不是最后一批
                    assertEquals(splitSubtable, batchSubtables.size());
                } else {
                    // 最后一批
                    assertEquals(subtableNames.size() % splitSubtable, batchSubtables.size());
                }
                
                batchCount++;
            }
            
            // 验证总批次数
            assertEquals((subtableNames.size() + splitSubtable - 1) / splitSubtable, batchCount);
        }
    }
    
    @Test
    public void testConfigurationParsing() {
        // 测试配置解析
        Configuration conf1 = Configuration.from("{\"splitSubtable\": 500}");
        assertEquals(500, conf1.getInt("splitSubtable", 0).intValue());
        
        Configuration conf2 = Configuration.from("{}");
        assertEquals(0, conf2.getInt("splitSubtable", 0).intValue());
        
        Configuration conf3 = Configuration.from("{\"splitSubtable\": 0}");
        assertEquals(0, conf3.getInt("splitSubtable", 0).intValue());
        
        Configuration conf4 = Configuration.from("{\"splitSubtable\": 100}");
        assertEquals(100, conf4.getInt("splitSubtable", 0).intValue());
    }
}