package com.yirendai.sqoop;

import com.beust.jcommander.JCommander;
import com.yirendai.sqoop.common.WorkloadReader;
import com.yirendai.sqoop.model.Config;
import com.yirendai.sqoop.model.Pair;
import com.yirendai.sqoop.scanner.CsvScanner;
import com.yirendai.sqoop.scanner.Scanner;
import com.yirendai.sqoop.worker.SqoopWorker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * App Class
 * @author Li He
 */
public class App {
    private static Log log = LogFactory.getLog(App.class);

    public static void main(String[] args) {
        Commands commands = new Commands();

        JCommander commander = new JCommander(commands, args);

        if (commands.help) {
            commander.usage();
            return;
        }

        // No action indicated, use worker
        if ("worker".equals(commands.action)) {
            log.info("Using configuration file: '" + commands.conf + "'");
            List<Config> configs;
            try {
                configs = WorkloadReader.parseConfig(commands.conf);
            } catch (IOException e) {
                log.error(e);
                return;
            }
            if (configs == null || configs.isEmpty()) {
                log.warn("No Workload Content has been parsed. Please Check if your file is empty.");
                return;
            }
            SqoopWorker worker = new SqoopWorker();

            List<Pair> pairs = worker.handleConfigs(configs);
            // start all jobs
            for (Pair pair : pairs) {
                if (pair.getKey().equals("job")) {
                    worker.startJob(pair.getValue());
                }
            }
        } else if ("scanner".equals(commands.action)){
            log.info("Using configuration file: '" + commands.conf + "'");
            if (commands.input == null || "".equals(commands.input)) {
                log.error("Please at lease provide the path of an input file");
                return;
            }
            Scanner scanner;
            List<Map<String, String>> configs;
            scanner = new CsvScanner(commands.input, commands.output);
            try {
                configs = WorkloadReader.read(commands.conf);
                scanner.scan(configs);
            } catch (IOException e) {
                log.error(e);
            }
        }
    }
}
