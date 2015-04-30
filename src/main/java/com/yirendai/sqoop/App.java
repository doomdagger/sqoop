package com.yirendai.sqoop;

import com.yirendai.sqoop.model.Config;
import com.yirendai.sqoop.model.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

/**
 * App Class
 * @author Li He
 */
public class App {
    private static Log log = LogFactory.getLog(App.class);

    public static void main(String[] args) {
        String filePath = null;

        if (args.length == 1) {
            filePath = args[0];
        }

        if (filePath == null || "".equals(filePath)) {
            log.info("You did not pass the custom path of *.conf file, so using the default path.");
            filePath = PropertyConfigurer.getProperty("workload.file");
        }

        log.info("Using configuration file: '" + filePath + "'");

        List<Config> configs;
        try {
            configs = WorkloadReader.parseConfig(filePath);
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
    }
}
