package com.yirendai.sqoop.model;

/**
 * Created by lihe on 4/29/15.
 * @author Li He
 */
public class Pair {
    private String key;
    private long value;

    public Pair(String key, long value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}
