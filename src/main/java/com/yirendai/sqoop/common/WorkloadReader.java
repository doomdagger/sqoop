package com.yirendai.sqoop.common;

import com.yirendai.sqoop.model.Config;
import com.yirendai.sqoop.model.JobConfig;
import com.yirendai.sqoop.model.LinkConfig;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by lihe on 4/29/15.
 * @author Li He
 */
public class WorkloadReader {
    // Config Keys for Link

    // common
    public static final String LINK_CONFIG_CID = "linkConfig.cid";
    public static final String LINK_CONFIG_NAME= "linkConfig.name";
    public static final String LINK_CONFIG_CREATION_USER = "linkConfig.creationUser";

    // jdbc
    public static final String LINK_CONFIG_CONNECTION_STRING = "linkConfig.connectionString";
    public static final String LINK_CONFIG_JDBC_DRIVER = "linkConfig.jdbcDriver";
    public static final String LINK_CONFIG_USERNAME = "linkConfig.username";
    public static final String LINK_CONFIG_PASSWORD = "linkConfig.password";

    // hdfs
    public static final String LINK_CONFIG_URI = "linkConfig.uri";

    // Config Keys for Job

    // common
    public static final String JOB_CONFIG_FROM_LINK_NAME = "jobConfig.fromLinkName";
    public static final String JOB_CONFIG_TO_LINK_NAME = "jobConfig.toLinkName";
    public static final String JOB_CONFIG_NAME = "jobConfig.name";
    public static final String JOB_CONFIG_CREATION_USER = "jobConfig.creationUser";

    // from config
    public static final String FROM_JOB_CONFIG_SCHEMA_NAME = "fromJobConfig.schemaName";
    public static final String FROM_JOB_CONFIG_TABLE_NAME = "fromJobConfig.tableName";
    public static final String FROM_JOB_CONFIG_PARTITION_COLUMN = "fromJobConfig.partitionColumn";
    public static final String FROM_JOB_CONFIG_SQL = "fromJobConfig.sql";
    public static final String FROM_JOB_CONFIG_COLUMNS = "fromJobConfig.columns";
    public static final String FROM_JOB_CONFIG_STAGE_TABLE_NAME = "fromJobConfig.stageTableName";
    public static final String FROM_JOB_CONFIG_SHOULD_CLEAR_STAGE_TABLE = "fromJobConfig.shouldClearStageTable";

    // to config
    public static final String TO_JOB_CONFIG_OUTPUT_DIRECTORY = "toJobConfig.outputDirectory";

    // throttling config
    public static final String THROTTLING_CONFIG_NUM_EXTRACTORS = "throttlingConfig.numExtractors";
    public static final String THROTTLING_CONFIG_NUM_LOADERS = "throttlingConfig.numLoaders";

    /**
     * Expose Only this method
     * @param filePath workload file path
     * @return A List of Config Objects
     * @throws IOException
     */
    public static List<Config> parseConfig(String filePath) throws IOException {
        return parse(read(filePath));
    }

    public static List<Map<String, String>> read(String filePath) throws IOException {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath)));
        List<Map<String, String>> configs = new ArrayList<Map<String, String>>();

        String line;
        Map<String, String> config = null;
        String[] temp;
        boolean next = true;
        while((line=reader.readLine()) != null) {
            if (line.startsWith("#")) {
                // comment, skip it
                continue;
            }
            if ("".equals(line.trim())) {
                // empty line, new config start
                next = true;
                if (config != null) {
                    configs.add(config);
                    config = null;
                }
                continue;
            }
            if (next) {
                next = false;
                config = new LinkedHashMap<String, String>();
            }
            temp = line.split("=");
            if (temp.length != 2) {
                throw new RuntimeException("Input item: '" + line + "' is malformed!");
            }
            config.put(temp[0], temp[1]);
        }

        if (config != null) {
            configs.add(config);
        }

        return configs;
    }

    private static List<Config> parse(List<Map<String, String>> configs) {
        List<Config> configList = new ArrayList<Config>();

        for (Map<String, String> config : configs) {
            if (config.containsKey(LINK_CONFIG_CID)) {
                // it's a link definition
                configList.add(parseLinkConfig(config));
            } else if (config.containsKey(JOB_CONFIG_FROM_LINK_NAME)) {
                // it's a job definition
                configList.add(parseJobConfig(config));
            } else {
                throw new RuntimeException("Unrecognized Definition in workload file. " +
                        "You should at lease include key 'linkConfig.cid' or 'jobConfig.fromLinkId' " +
                        "to identify your definition.");
            }
        }

        return configList;
    }

    private static LinkConfig parseLinkConfig(Map<String, String> config) {
        LinkConfig linkConfig = new LinkConfig();
        linkConfig.setCid(Long.valueOf(config.get(LINK_CONFIG_CID)));
        linkConfig.setName(config.get(LINK_CONFIG_NAME));
        linkConfig.setCreationUser(config.get(LINK_CONFIG_CREATION_USER));

        linkConfig.addItem(LINK_CONFIG_CONNECTION_STRING, config.get(LINK_CONFIG_CONNECTION_STRING));
        linkConfig.addItem(LINK_CONFIG_JDBC_DRIVER, config.get(LINK_CONFIG_JDBC_DRIVER));
        linkConfig.addItem(LINK_CONFIG_USERNAME, config.get(LINK_CONFIG_USERNAME));
        linkConfig.addItem(LINK_CONFIG_PASSWORD, config.get(LINK_CONFIG_PASSWORD));
        linkConfig.addItem(LINK_CONFIG_URI, config.get(LINK_CONFIG_URI));

        return linkConfig;
    }

    private static JobConfig parseJobConfig(Map<String, String> config) {
        JobConfig jobConfig = new JobConfig();

        jobConfig.setFrom(config.get(JOB_CONFIG_FROM_LINK_NAME));
        jobConfig.setTo(config.get(JOB_CONFIG_TO_LINK_NAME));
        jobConfig.setName(config.get(JOB_CONFIG_NAME));
        jobConfig.setCreationUser(config.get(JOB_CONFIG_CREATION_USER));

        jobConfig.addFromItem(FROM_JOB_CONFIG_SCHEMA_NAME, config.get(FROM_JOB_CONFIG_SCHEMA_NAME));
        jobConfig.addFromItem(FROM_JOB_CONFIG_PARTITION_COLUMN, config.get(FROM_JOB_CONFIG_PARTITION_COLUMN));
        jobConfig.addFromItem(FROM_JOB_CONFIG_SQL, config.get(FROM_JOB_CONFIG_SQL));
        jobConfig.addFromItem(FROM_JOB_CONFIG_TABLE_NAME, config.get(FROM_JOB_CONFIG_TABLE_NAME));
        jobConfig.addFromItem(FROM_JOB_CONFIG_STAGE_TABLE_NAME, config.get(FROM_JOB_CONFIG_STAGE_TABLE_NAME));
        jobConfig.addFromItem(FROM_JOB_CONFIG_SHOULD_CLEAR_STAGE_TABLE, config.get(FROM_JOB_CONFIG_SHOULD_CLEAR_STAGE_TABLE));
        jobConfig.addFromItem(FROM_JOB_CONFIG_COLUMNS, config.get(FROM_JOB_CONFIG_COLUMNS));

        // handle date format
        String directory = config.get(TO_JOB_CONFIG_OUTPUT_DIRECTORY);
        String parsedDirectory = directory;
        if (directory != null) {
            Date date = new Date();
            Pattern pattern = Pattern.compile("\\[([^\\]]+)\\]");
            Matcher matcher = pattern.matcher(directory);
            int endIndex = 0;
            while(matcher.find(endIndex)) {
                parsedDirectory = parsedDirectory.replace(matcher.group(0), Utils.parseDate(date, matcher.group(1)));
                endIndex = matcher.end(0);
            }
        }
        jobConfig.addToItem(TO_JOB_CONFIG_OUTPUT_DIRECTORY, parsedDirectory);

        jobConfig.addDriverItem(THROTTLING_CONFIG_NUM_EXTRACTORS, config.get(THROTTLING_CONFIG_NUM_EXTRACTORS));
        jobConfig.addDriverItem(THROTTLING_CONFIG_NUM_LOADERS, config.get(THROTTLING_CONFIG_NUM_LOADERS));

        return jobConfig;
    }

    public static Map<String, String> parseJob(Map<String, String> jobConfig, Map<String, String> dedicatedValue) {
        if (!jobConfig.containsKey(JOB_CONFIG_FROM_LINK_NAME)) {
            throw new RuntimeException("A defined job without from Link Name is invalid!");
        }
        Map<String, String> parsedConfig = new LinkedHashMap<String, String>();

        Pattern pattern = Pattern.compile("\\{([^\\}]+)\\}");

        for (Map.Entry<String, String> entry : jobConfig.entrySet()) {
            String value = jobConfig.get(entry.getKey());
            String parsedValue = value;
            Matcher matcher = pattern.matcher(value);
            int endIndex = 0;
            while(matcher.find(endIndex)) {
                String dvalue = dedicatedValue.get(matcher.group(1));
                if (dvalue == null || "".equals(dvalue)) {
                    throw new RuntimeException("The dedicated key '" + matcher.group(1) + "' does not exist! " +
                            "It may be caused by the fact that your input file does not support this dedicated key.");
                }
                parsedValue = parsedValue.replace(matcher.group(0), dvalue);
                endIndex = matcher.end(0);
            }
            parsedConfig.put(entry.getKey(), parsedValue);
        }

        return parsedConfig;
    }

}
