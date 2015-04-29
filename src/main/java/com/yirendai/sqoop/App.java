package com.yirendai.sqoop;

import com.yirendai.sqoop.model.Config;
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
            filePath = PropertyConfigurer.getProperty("workload.file");
        }

        List<Config> configs = null;
        try {
            configs = WorkloadReader.parseConfig(filePath);
        } catch (IOException e) {
            log.error(e);
            return;
        }

        if (configs == null || configs.isEmpty()) {
            log.info("No Workload Content has been parsed. Please Check if your file is empty.");
            return;
        }

        //SqoopWorker worker = new SqoopWorker();


    }
}
