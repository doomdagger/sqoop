package com.yirendai.sqoop.scanner;

import com.yirendai.sqoop.common.WorkloadReader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.yirendai.sqoop.common.WorkloadReader.*;

/**
 * Created by lihe on 4/30/15.
 * @author Li He
 */
public class CsvScanner extends Scanner {

    public CsvScanner(String inputFile) {
        super(inputFile);
    }

    public CsvScanner(String inputFile, String outputFile) {
        super(inputFile, outputFile);
    }

    @Override
    public void scan(List<Map<String, String>> configs) throws IOException {
        BufferedReader reader = this.getReader();
        BufferedWriter writer = this.getWriter();

        List<Map<String, String>> jobConfigs = new ArrayList<Map<String, String>>();

        for (Map<String, String> config : configs) {
            if (!config.containsKey(JOB_CONFIG_FROM_LINK_NAME)) {
                for (Map.Entry<String, String> entry : config.entrySet()) {
                    writer.write(entry.getKey() + "=" + entry.getValue());
                    writer.newLine();
                }
                writer.newLine();
            } else {
                jobConfigs.add(config);
            }
        }

        String line;
        while ((line = reader.readLine()) != null) {
            if ("".equals(line.trim())) {
                continue;
            }
            // "C_CONTACT_INFO_CREDITAUDIT","I_CONTACT_ID"
            line = line.replaceAll("[\"\']", "");
            String[] parts = line.split(",");

            Map<String, String> dedicatedValue = new HashMap<String, String>();
            dedicatedValue.put(FROM_JOB_CONFIG_TABLE_NAME, parts[0]);
            dedicatedValue.put(FROM_JOB_CONFIG_PARTITION_COLUMN, parts[1]);
            for (Map<String, String> config : jobConfigs) {
                for (Map.Entry<String, String> entry : WorkloadReader.parseJob(config, dedicatedValue).entrySet()) {
                    writer.write(entry.getKey() + "=" + entry.getValue());
                    writer.newLine();
                }
                writer.newLine();
            }
        }

        cleanup();
    }
}
