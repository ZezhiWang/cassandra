package org.apache.cassandra.service;

import org.apache.cassandra.cql3.ColumnIdentifier;

public class TreasConsts {

    public static final String TAG = "tag";
    public static final String TAG_ONE  = "tag1";
    public static final String TAG_TWO  = "tag2";
    public static final String TAG_THREE  = "tag3";

    public static final String VAL =  "field0";
    public static final String VAL_ONE =  "field1";
    public static final String VAL_TWO =  "field2";
    public static final String VAL_THREE =  "field3";

    public static  ColumnIdentifier ORIGINIAL_TAG_IDENTIFIER = new ColumnIdentifier(TAG, true);
    public static  ColumnIdentifier TAG_ONE_IDENTIFIER = new ColumnIdentifier(TAG_ONE, true);
    public static  ColumnIdentifier TAG_TWO_IDENTIFIER = new ColumnIdentifier(TAG_TWO, true);
    public static  ColumnIdentifier TAG_THREE_IDENTIFIER = new ColumnIdentifier(TAG_THREE, true);
    
    public static ColumnIdentifier ORIGINAL_VAL_IDENTIFIER = new ColumnIdentifier(VAL, true);
    public static  ColumnIdentifier VAL_ONE_IDENTIFIER = new ColumnIdentifier(VAL_ONE, true);
    public static  ColumnIdentifier VAL_TWO_IDENTIFIER = new ColumnIdentifier(VAL_TWO, true);
    public static  ColumnIdentifier VAL_THREE_IDENTIFIER = new ColumnIdentifier(VAL_THREE, true);

    public static int K = 3;
    public static int L = 3;

}
