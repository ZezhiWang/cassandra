package org.apache.cassandra.service;

public class TagVal {
    public TreasTag tag;
    public String val;

    public TagVal(TreasTag t, String v){
        this.tag = t;
        this.val = v;
    }
}
