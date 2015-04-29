package com.yirendai.sqoop;

import com.yirendai.sqoop.model.Config;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;

import java.util.List;

/**
 * Created by lihe on 4/29/15.
 * @author Li He
 */
public class SqoopWorker {

    private static Log log = LogFactory.getLog(SqoopWorker.class);

    private SqoopClient client;

    public SqoopWorker() {
        this.client = new SqoopClient(PropertyConfigurer.getProperty("server.url"));
    }

    public void handleConfig(Config config) {

    }

    public MLink createLink() {
        return null;
    }

    public List<MLink> getLinks() {
        return client.getLinks();
    }

    public MLink getLink(long lid) {
        return client.getLink(lid);
    }

    public MJob createJob() {
        return null;
    }

    public List<MJob> getJobs() {
        return client.getJobs();
    }

    public MJob getJob(long jid) {
        return client.getJob(jid);
    }

    public boolean updateLink(MLink link) {
        return false;
    }

    public boolean updateJob(MJob job) {
        return false;
    }

    public boolean deleteLink(long lid) {
        return false;
    }

    public boolean deleteJob(long jid) {
        return false;
    }
}
