package com.liweidao.apps.UTF;

import com.liweidao.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Desc: 自定义UDTF函数实现分词功能
 */
//分词udf
@FunctionHint(output = @DataTypeHint("ROW<s STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String value) {
        List<String> words = KeywordUtil.analyze(value);
        for (String word : words) {
            Row row = new Row(1);
            row.setField(0, word);
            collect(row);
        }
    }
}
