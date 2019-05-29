package com.example.hbaseclient.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.hbaseclient.configuration.TableUtils;
import org.apache.avro.data.Json;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@RestController
@RequestMapping("/test")
public class TestController {

    private static final String CHARSET = "UTF-8";

    @Autowired
    private Admin admin;

    @RequestMapping("/exist")
    public String exist(@RequestParam String tableName) throws IOException {
        boolean result = admin.tableExists(TableName.valueOf(tableName));
        return "result:" + result;
    }

    /**
     * @param tableName
     * @param columnFamilies 格式：列族1,列族2,.....
     * @return
     */
    @RequestMapping("/create")
    public String create(@RequestParam String tableName, @RequestParam String columnFamilies) throws IOException {
        String[] cfs = columnFamilies.split(",");
        List<ColumnFamilyDescriptor> cfds = new ArrayList<>();
        for (String cf : cfs) {
            cfds.add(ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build());
        }
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                .setColumnFamilies(cfds).build();
        admin.createTable(tableDescriptor);
        return "success";
    }

    @RequestMapping("/delete")
    public String delete(@RequestParam String tableName) throws IOException {
        if (admin.tableExists(TableName.valueOf(tableName))) {
            if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
            }
            System.out.println(tableName + "表卸载成功=========");
            admin.deleteTable(TableName.valueOf(tableName));
        }
        return "success";
    }

    @PostMapping("/addRow")
    public String addRow(@RequestParam String tableName, @RequestParam String rowKey,
                         @RequestParam String cf, @RequestParam String column, @RequestParam String content) {
        String result = "success";
        try {
            Table table = TableUtils.getHTable(tableName);
            //put
            Put put = new Put(rowKey.getBytes());
            put.addColumn(cf.getBytes(),column.getBytes(),content.getBytes());
            table.put(put);
        } catch (IOException e) {
            result = e.getMessage();
        }
        return result;
    }

    @PostMapping("/deleteRow")
    public String deleteRow(@RequestParam String tableName,@RequestParam String rowKey,
                            @RequestParam String cf,@RequestParam String column) {
        String result = "success";

        try {
            Table table = TableUtils.getHTable(tableName);
            Delete delete = new Delete(rowKey.getBytes());
            delete.addColumn(cf.getBytes(),column.getBytes());
            table.delete(delete);
        } catch (IOException e) {
            result = e.getMessage();
        }

        return result;
    }

    @PostMapping("/scan")
    public String scan(@RequestParam String tableName , @RequestParam String startRow ,
                       @RequestParam String stopRow,@RequestParam String cf,@RequestParam String column) {
        JSONObject response = new JSONObject();
        JSONArray cellArr = new JSONArray();
        response.put("tableName",tableName);
        response.put("cellArr",cellArr);
        try {
            Table table = TableUtils.getHTable(tableName);
            //除了扫描全局和扫描一行，感觉看不到其他的方法了
            Scan scan = new Scan();
            scan.addColumn(cf.getBytes(),column.getBytes());
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> tableResult = scanner.iterator();
            while (tableResult.hasNext()) {
                Result result = tableResult.next();
                Cell[] cells = result.rawCells();
                for(Cell cell : cells) {
                    JSONObject cellJson = new JSONObject();
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String clm = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String content = Bytes.toString(CellUtil.cloneValue(cell));
                    cellJson.put("family",family);
                    cellJson.put("column",clm);
                    cellJson.put("content",content);
                    cellArr.add(cellJson);
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return response.toJSONString();
    }

}
