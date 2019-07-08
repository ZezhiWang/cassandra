package org.apache.cassandra.service;

import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class PersonalizedLogger {
    private Object obj1 = new Object();
    private Object obj2 = new Object();
    private Object obj3 = new Object();
    private Object obj4 = new Object();
    private Object obj5 = new Object();
    private Object obj6 = new Object();

    private static final Logger logger = LoggerFactory.getLogger(PersonalizedLogger.class);

    private static PersonalizedLogger sw = new PersonalizedLogger();

    public static PersonalizedLogger getLogTime() {
        return sw;
    }

    private final String absPath = "/root/cassandra/logs/";
    private String filePrefix = replaceDot(FBUtilities.getJustLocalAddress().toString());


    private String replaceDot(String name) {
        String s = name.replace(".","");
        logger.debug("The trim string is" + s);
        return s;
    }

    private  void initFile(String name){
        File file = new File(name);
        try
        {
            file.createNewFile();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

//    public void replicaPerform(long num) {
//        synchronized (obj6) {
//            int index = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
//            String name = absPath + "ReplicaResp" + (index + 1) + ".txt";
//            FileWriter writer = null;
//            try
//            {
//                initFile(name);
//                writer = new FileWriter(name,true);
//            } catch  (IOException e) {
//                e.printStackTrace();
//            }
//            BufferedWriter printWriter = new BufferedWriter (writer);
//            try {
//                printWriter.write(num+"");
//                printWriter.newLine();
//                printWriter.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    public void readTag(long num) {
        synchronized (obj5) {
            String name = absPath + filePrefix + "ReadTag.txt";
            FileWriter writer = null;
            try
            {
                initFile(name);
                writer = new FileWriter(name,true);
            } catch  (IOException e) {
                e.printStackTrace();
            }
            BufferedWriter printWriter = new BufferedWriter (writer);
            try {
                printWriter.write(num+"");
                printWriter.newLine();
                printWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

//    public void readValue(long num) {
//        synchronized (obj5) {
//            int index = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
//            String name = absPath + "ReadValueWait" + (index + 1) + ".txt";
//            FileWriter writer = null;
//            try
//            {
//                initFile(name);
//                writer = new FileWriter(name,true);
//            } catch  (IOException e) {
//                e.printStackTrace();
//            }
//            BufferedWriter printWriter = new BufferedWriter (writer);
//            try {
//                printWriter.write(num+"");
//                printWriter.newLine();
//                printWriter.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public void writeReadValue(long num) {
//        synchronized (obj1) {
//            int index = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
//            String name = absPath + "ReadValueAll" + (index + 1) + ".txt";
//            FileWriter writer = null;
//            try
//            {
//                initFile(name);
//                writer = new FileWriter(name,true);
//            } catch  (IOException e) {
//                e.printStackTrace();
//            }
//            BufferedWriter printWriter = new BufferedWriter (writer);
//            try {
//                printWriter.write(num+"");
//                printWriter.newLine();
//                printWriter.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public void writeReadTag(long num) {
//        synchronized (obj2) {
//            int index = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
//            String name = absPath + "ReadTagAll" + (index + 1) + ".txt";
//            FileWriter writer = null;
//            try
//            {
//                initFile(name);
//                writer = new FileWriter(name,true);
//            } catch  (IOException e) {
//                e.printStackTrace();
//            }
//            BufferedWriter printWriter = new BufferedWriter (writer);
//            try {
//                printWriter.write(num+"");
//                printWriter.newLine();
//                printWriter.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public void writeMutationMain(long num) {
//        synchronized (obj3) {
//            int index = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
//            String name = absPath + "writeMutationMain" + (index + 1) + ".txt";
//            FileWriter writer = null;
//            try
//            {
//                initFile(name);
//                writer = new FileWriter(name,true);
//            } catch  (IOException e) {
//                e.printStackTrace();
//            }
//            BufferedWriter printWriter = new BufferedWriter (writer);
//            try {
//                printWriter.write(num+"");
//                printWriter.newLine();
//                printWriter.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//
//    public void writeMutationVerb(long num) {
//        synchronized (obj4) {
//            int index = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
//            String name = absPath + "MutationVerb" + (index + 1) + ".txt";
//            FileWriter writer = null;
//            try
//            {
//                initFile(name);
//                writer = new FileWriter(name,true);
//            } catch  (IOException e) {
//                e.printStackTrace();
//            }
//            BufferedWriter printWriter = new BufferedWriter (writer);
//            try {
//                printWriter.write(num+"");
//                printWriter.newLine();
//                printWriter.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
}
