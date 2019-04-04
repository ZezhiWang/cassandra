/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.treas;

import org.apache.cassandra.cql3.ColumnIdentifier;

public class TreasConsts {

    public static int K = 3;
    public static int L = 2;

    public static final TreasConfig CONFIG = new TreasConfig(K);
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


}
