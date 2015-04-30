package com.yirendai.sqoop.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lihe on 4/29/15.
 * @author Li He
 */
public class JobConfig implements Config{
    private long from;
    private long to;
    private String name;
    private String creationUser;

    private Map<String, String> fromItems = new HashMap<String, String>();
    private Map<String, String> toItems = new HashMap<String, String>();
    private Map<String, String> driverItems = new HashMap<String, String>();

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCreationUser() {
        return creationUser;
    }

    public void setCreationUser(String creationUser) {
        this.creationUser = creationUser;
    }

    public Map<String, String> getFromItems() {
        return fromItems;
    }

    public void setFromItems(Map<String, String> fromItems) {
        this.fromItems = fromItems;
    }

    public Map<String, String> getToItems() {
        return toItems;
    }

    public void setToItems(Map<String, String> toItems) {
        this.toItems = toItems;
    }

    public Map<String, String> getDriverItems() {
        return driverItems;
    }

    public void setDriverItems(Map<String, String> driverItems) {
        this.driverItems = driverItems;
    }

    public void addFromItem(String key, String value) {
        if (value == null || "".equals(value)) {
            return;
        }
        fromItems.put(key, value);
    }

    public void addToItem(String key, String value) {
        if (value == null || "".equals(value)) {
            return;
        }
        toItems.put(key, value);
    }

    public void addDriverItem(String key, String value) {
        if (value == null || "".equals(value)) {
            return;
        }
        driverItems.put(key, value);
    }

    @Override
    public String toString() {
        return "\n" +
                this.getFrom() + "\n" +
                this.getTo() + "\n" +
                this.getName() + "\n" +
                this.getCreationUser() + "\n" +
                this.getFromItems() + "\n" +
                this.getToItems() + "\n" +
                this.getDriverItems();
    }
}
