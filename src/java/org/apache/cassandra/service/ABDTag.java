package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.io.Serializable;
import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ABDTag implements Serializable{
    private int logicalTIme;
    private String writerId ;
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    public ABDTag(){
        this.logicalTIme = -1;
        this.writerId = FBUtilities.getLocalAddressAndPort().toString(false);
    }

    private ABDTag(String tagString){
        String[] tagArray = tagString.split(";");
        this.logicalTIme = Integer.parseInt(tagArray[0]);
        this.writerId = tagArray[1];
    }


    public int getTime(){
        return logicalTIme;
    }

    public String getWriterId() {
        return writerId;
    }

    public ABDTag nextTag(){
        this.logicalTIme++;
        return this;
    }

    public static String serialize(ABDTag tag) {
        return tag.toString();
    }

    public static ABDTag deserialize(ByteBuffer buf) {
        String tagString = "";
        try {
            tagString = ByteBufferUtil.string(buf);
        } catch (CharacterCodingException e){
            logger.warn("err casting tag {}",e);
            return new ABDTag();
        }

        return new ABDTag(tagString);
    }

    public boolean isLarger(ABDTag other){
        if(this.logicalTIme != other.getTime()){
            return this.logicalTIme - other.getTime() > 0;
        } else {
            return this.writerId.compareTo(other.getWriterId()) > 0;
        }
    }

    public String toString() {
        return logicalTIme + ";" + writerId;
    }
}
