package com.yirendai.sqoop.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lihe on 4/29/15.
 * @author Li He
 */
public class LinkConfig implements Config{
    private long cid;
    private String name;
    private String creationUser;
    private Map<String, String> items = new HashMap<String, String>();

    public long getCid() {
        return cid;
    }

    public void setCid(long cid) {
        this.cid = cid;
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

    public Map<String, String> getItems() {
        return items;
    }

    public void setItems(Map<String, String> items) {
        this.items = items;
    }

    public void addItem(String key, String value) {
        if (value == null || "".equals(value)) {
            return;
        }
        items.put(key, value);
    }

    @Override
    public String toString() {
        return "\n" + this.getCid() + "\n" +
                this.getName() + "\n" +
                this.getCreationUser() + "\n" +
                this.getItems();
    }
}
