package com.yirendai.sqoop;

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
        if (className.equals(LINK_CONFIG_CLASS)) {
            LinkConfig linkConfig = (LinkConfig)config;

            // find the link by name
            MLink link = this.getLink(linkConfig.getName());
            if (link != null) {
                // we have the link with this name
                // try to do the update job
                if (link.getConnectorId() != linkConfig.getCid()) {
                    throw new RuntimeException("The Link with name '"
                            + linkConfig.getName() + " is already exists, albeit having " +
                            "different connector id. Is this a typo? If not, please change your link name.");
                }
                MLinkConfig mlc = link.getConnectorLinkConfig();
                // fill in the link config values
                for (Map.Entry<String, String> entry : linkConfig.getItems().entrySet()) {
                    mlc.getStringInput(entry.getKey()).setValue(entry.getValue());
                }
                Status status = this.client.updateLink(link);
                if(status.canProceed()) {
                    log.info("Updated Link with Link Id : " + link.getPersistenceId());
                } else {
                    throw new RuntimeException("Something went wrong updating the link '" + link.getName() + "'");
                }
            } else {
                // we don't have the link with this name
                // try to create a new one
                link = this.client.createLink(linkConfig.getCid());
                link.setName(linkConfig.getName());
                link.setCreationUser(linkConfig.getCreationUser());
                MLinkConfig mlc = link.getConnectorLinkConfig();
                // fill in the link config values
                for (Map.Entry<String, String> entry : linkConfig.getItems().entrySet()) {
                    mlc.getStringInput(entry.getKey()).setValue(entry.getValue());
                }
                Status status = this.client.saveLink(link);
                if(status.canProceed()) {
                    log.info("Created Link with Link Id : " + link.getPersistenceId());
                } else {
                    throw new RuntimeException("Something went wrong creating the link '" + link.getName() + "'");
                }
            }
            return new Pair("link", link.getPersistenceId());
        } else if (className.equals(JOB_CONFIG_CLASS)) {
            JobConfig jobConfig = (JobConfig)config;

            MJob job = this.getJob(jobConfig.getName());
            if (job != null) {
                if (job.getLinkId(Direction.FROM) != jobConfig.getFrom()
                        || job.getLinkId(Direction.TO) != jobConfig.getTo()) {
                    throw new RuntimeException("The Job with same name exists, but it has different link ids.");
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
                Status status = this.client.updateJob(job);
                if(status.canProceed()) {
                    log.info("Updated Job with Link Id : " + job.getPersistenceId());
                } else {
                    throw new RuntimeException("Something went wrong updating the link '" + job.getName() + "'");
                }
            } else {
                //Creating dummy job object
                long fromLinkId = jobConfig.getFrom();// for jdbc connector
                long toLinkId = jobConfig.getTo(); // for HDFS connector
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
                Status status = this.client.updateJob(job);
                if(status.canProceed()) {
                    log.info("Created Job with Link Id : " + job.getPersistenceId());
                } else {
                    throw new RuntimeException("Something went wrong creating the link '" + job.getName() + "'");
                }
            }
            return new Pair("job", job.getPersistenceId());
        } else {
            throw new RuntimeException(
                    "Unknown Config Subclass, Perhaps you forget to update the registered class list");
        }
    }

    public List<Pair> handleConfigs(List<Config> configs) {
        List<Pair> pairs = new ArrayList<Pair>();
        for (Config config : configs) {
            pairs.add(handleConfig(config));
        }
        return pairs;
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
