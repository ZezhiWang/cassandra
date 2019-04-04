package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.io.Serializable;
import java.nio.charset.CharacterCodingException;
import java.util.List;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreasTag implements Serializable{
    private int logicalTIme;
    private String writerId ;
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    public TreasTag(){
        this.logicalTIme = -1;
        this.writerId = FBUtilities.getLocalAddressAndPort().toString(false);
        logger.info(this.toString());
    }

    private TreasTag(String tagString){
        String[] tagArray = tagString.split(";");
        this.logicalTIme = Integer.parseInt(tagArray[0]);
        this.writerId = tagArray[1];
        logger.info(this.toString());
    }


    public int getTime(){
        return logicalTIme;
    }

    public String getWriterId() {
        return writerId;
    }

    public TreasTag nextTag(){
        this.logicalTIme++;
        logger.info(this.toString());
        return this;
    }

    public static String serializeHelper(TreasTag tag) {
        return tag.toString();
    }

    public static TreasTag deserialize(ByteBuffer buf) {
        String tagString = "";
        try {
            tagString = ByteBufferUtil.string(buf);
        } catch (CharacterCodingException e){
            logger.warn("err casting tag {}",e);
            return new TreasTag();
        }

        return new TreasTag(tagString);
    }

    public boolean isLarger(TreasTag other){
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