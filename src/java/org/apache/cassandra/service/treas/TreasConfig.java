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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cql3.ColumnIdentifier;

public class TreasConfig
{
    public static final String ORIGINAL_VAl = "field";
    public static final String ORIGINAL_TAG = "tag";

    public static final ColumnIdentifier CI_TAG = new ColumnIdentifier(ORIGINAL_TAG,true);
    public static final ColumnIdentifier CI_VAL = new ColumnIdentifier(ORIGINAL_VAl,true);


    public Map<String,String> tagToVal;
    public Map<String,String> valToTag;
    public Map<String, ColumnIdentifier> tagToIdentifier;
    public Map<String, ColumnIdentifier> valToIdentifier;

    public TreasConfig(int K)
    {
        for(int r = 0; r<K; r++){
            String newTag = ORIGINAL_TAG + r;
            String newVal = ORIGINAL_VAl + r;
            tagToVal.put(newTag,newVal);
            valToTag.put(newVal,newTag);
            ColumnIdentifier newTagIdentifier = new ColumnIdentifier(newTag, true);
            ColumnIdentifier newValIdentifier = new ColumnIdentifier(newVal, true);
            valToIdentifier.put(newVal,newValIdentifier);
            tagToIdentifier.put(newTag,newTagIdentifier);
        }

    }

    public Collection<ColumnIdentifier> tagIdentifiers(){
        return tagToIdentifier.values();
    }

    public Set<String> returnTags(){return tagToIdentifier.keySet();}

    public String getVal(String tag){return  tagToVal.get(tag);}

    public Set<Map.Entry<String,String>> getTagToIdValSet(){return tagToVal.entrySet(); }



}
