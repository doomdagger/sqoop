package com.yirendai.sqoop.worker;

import com.yirendai.sqoop.model.Config;
import com.yirendai.sqoop.model.JobConfig;
import com.yirendai.sqoop.model.LinkConfig;
import com.yirendai.sqoop.model.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by lihe on 4/29/15.
 * @author Li He
 */
public class SqoopWorker {
    private static final String LINK_CONFIG_CLASS = "com.yirendai.sqoop.model.LinkConfig";
    private static final String JOB_CONFIG_CLASS = "com.yirendai.sqoop.model.JobConfig";

    private static Log log = LogFactory.getLog(SqoopWorker.class);

    private SqoopClient client;

    public SqoopWorker() {
        this.client = new SqoopClient(PropertyConfigurer.getProperty("server.url"));
    }

    public Pair handleConfig(Config config) {
        String className = config.getClass().getName();
        Pair pair;

        if (className.equals(LINK_CONFIG_CLASS)) {
            LinkConfig linkConfig = (LinkConfig)config;

            pair = handleLinkConfig(linkConfig);
        } else if (className.equals(JOB_CONFIG_CLASS)) {
            JobConfig jobConfig = (JobConfig)config;

            pair = handleJobConfig(jobConfig);
        } else {
            throw new RuntimeException(
                    "Unknown Config Subclass, Perhaps you forget to update the registered class list");
        }
        return pair;
    }

    public List<Pair> handleConfigs(List<Config> configs) {
        List<Pair> pairs = new ArrayList<Pair>();
        for (Config config : configs) {
            pairs.add(handleConfig(config));
        }
        return pairs;
    }

    public Pair handleLinkConfig(LinkConfig linkConfig) {
        // find the link by name
        MLink link = this.getLink(linkConfig.getName());
        Status status;
        if (link != null) {
            // we have the link with this name
            // try to do the update link
            log.info("The Link with the given name '" + linkConfig.getName() + "' exists. Try updating...");
            if (link.getConnectorId() != linkConfig.getCid()) {
                throw new RuntimeException("The Link with name '"
                        + linkConfig.getName() + " is already exists, albeit having " +
                        "different connector id. Is this a typo? If not, please change your link name in order to create new one.");
            }
            MLinkConfig mlc = link.getConnectorLinkConfig();
            // fill in the link config values
            for (Map.Entry<String, String> entry : linkConfig.getItems().entrySet()) {
                mlc.getStringInput(entry.getKey()).setValue(entry.getValue());
            }
            status = this.client.updateLink(link);
        } else {
            // we don't have the link with this name
            // try to create a new one
            log.info("The Link with the given name '" + linkConfig.getName() + "' does not exist. Try creating one...");
            link = this.client.createLink(linkConfig.getCid());
            link.setName(linkConfig.getName());
            link.setCreationUser(linkConfig.getCreationUser());
            MLinkConfig mlc = link.getConnectorLinkConfig();
            // fill in the link config values
            for (Map.Entry<String, String> entry : linkConfig.getItems().entrySet()) {
                mlc.getStringInput(entry.getKey()).setValue(entry.getValue());
            }
            status = this.client.saveLink(link);
        }
        if(status.canProceed()) {
            log.info("Finished. The Link with Link Id : " + link.getPersistenceId());
        } else {
            throw new RuntimeException("Something went wrong dealing the link '" + link.getName() + "'");
        }
        return new Pair("link", link.getPersistenceId());
    }

    public Pair handleJobConfig(JobConfig jobConfig) {
        // find the job by name
        MJob job = this.getJob(jobConfig.getName());
        Status status;

        if (job != null) {
            // we have the job with this name
            // try to do the update job
            log.info("The Link with the given name '" + jobConfig.getName() + "' exists. Try updating...");
            if (job.getLinkId(Direction.FROM) != this.getLinkIdByName(jobConfig.getFrom())
                    || job.getLinkId(Direction.TO) != this.getLinkIdByName(jobConfig.getTo())) {
                throw new RuntimeException(
                        "The Job with same name exists, albeit having different link ids. Or the Link Name you given does not exist. " +
                                "Is this a typo? If not, please change your link name in order to create new one.");
            }
            // set the "FROM" link job config values
            MFromConfig fromJobConfig = job.getFromJobConfig();
            for (Map.Entry<String, String> entry : jobConfig.getFromItems().entrySet()) {
                fromJobConfig.getStringInput(entry.getKey()).setValue(entry.getValue());
            }
            // set the "TO" link job config values
            MToConfig toJobConfig = job.getToJobConfig();
            for (Map.Entry<String, String> entry : jobConfig.getToItems().entrySet()) {
                toJobConfig.getStringInput(entry.getKey()).setValue(entry.getValue());
            }
            // set the driver config values
            MDriverConfig driverConfig = job.getDriverConfig();
            for (Map.Entry<String, String> entry : jobConfig.getDriverItems().entrySet()) {
                driverConfig.getStringInput(entry.getKey()).setValue(entry.getValue());
            }
            status = this.client.updateJob(job);
        } else {
            // we don't have the link with this name
            // try to create a new one
            log.info("The Job with the given name '" + jobConfig.getName() + "' does not exist. Try creating one...");
            //Creating dummy job object
            long fromLinkId = this.getLinkIdByName(jobConfig.getFrom());// for jdbc connector
            long toLinkId = this.getLinkIdByName(jobConfig.getTo()); // for HDFS connector

            if (fromLinkId == -1 || toLinkId == -1) {
                throw new RuntimeException("Cannot find Link for the Link Name given by Job Configuration.");
            }

            job = client.createJob(fromLinkId, toLinkId);
            job.setName(jobConfig.getName());
            job.setCreationUser(jobConfig.getCreationUser());
            // set the "FROM" link job config values
            MFromConfig fromJobConfig = job.getFromJobConfig();
            for (Map.Entry<String, String> entry : jobConfig.getFromItems().entrySet()) {
                fromJobConfig.getStringInput(entry.getKey()).setValue(entry.getValue());
            }
            // set the "TO" link job config values
            MToConfig toJobConfig = job.getToJobConfig();
            for (Map.Entry<String, String> entry : jobConfig.getToItems().entrySet()) {
                toJobConfig.getStringInput(entry.getKey()).setValue(entry.getValue());
            }
            // set the driver config values
            MDriverConfig driverConfig = job.getDriverConfig();
            for (Map.Entry<String, String> entry : jobConfig.getDriverItems().entrySet()) {
                driverConfig.getStringInput(entry.getKey()).setValue(entry.getValue());
            }
            status = this.client.saveJob(job);
        }
        if(status.canProceed()) {
            log.info("The Job with Job Id : " + job.getPersistenceId());
        } else {
            throw new RuntimeException("Something went wrong dealing the job '" + job.getName() + "'");
        }
        return new Pair("job", job.getPersistenceId());
    }

    public List<MLink> getLinks() {
        return client.getLinks();
    }

    public MLink getLink(long lid) {
        return client.getLink(lid);
    }

    public MLink getLink(String name) {
        for (MLink link : this.getLinks()) {
            if (link.getName().equals(name)) {
                return link;
            }
        }
        return null;
    }

    public long getLinkIdByName(String name) {
        for (MLink link : this.getLinks()) {
            if (link.getName().equals(name)) {
                return link.getPersistenceId();
            }
        }
        return -1;
    }

    public List<MJob> getJobs() {
        return client.getJobs();
    }

    public MJob getJob(long jid) {
        return client.getJob(jid);
    }

    public MJob getJob(String name) {
        for (MJob job : this.getJobs()) {
            if (job.getName().equals(name)) {
                return job;
            }
        }
        return null;
    }

    public void startJob(long jid) {
        //Job start
        log.info("Starting Job with Job Id: " + jid);
        MSubmission submission = client.startJob(jid);
        log.info("Job Submission Status : " + submission.getStatus());
        if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
            log.info("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
        }
        log.info("Hadoop job id :" + submission.getExternalId());
        log.info("Job link : " + submission.getExternalLink());
        Counters counters = submission.getCounters();
        if(counters != null) {
            log.info("Counters:");
            for(CounterGroup group : counters) {
                System.out.print("\t");
                log.info(group.getName());
                for(Counter counter : group) {
                    System.out.print("\t\t");
                    System.out.print(counter.getName());
                    System.out.print(": ");
                    log.info(counter.getValue());
                }
            }
        }
        if(submission.getExceptionInfo() != null) {
            throw new RuntimeException("Exception info : " + submission.getExceptionInfo());
        }
    }
}
