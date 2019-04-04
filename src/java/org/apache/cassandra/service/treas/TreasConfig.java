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

package org.apache.cassandra.service.reads.treas;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.ColumnIdentifier;

public class TreasConfig
{

    public static final String Original_VAl = "field";
    public static final String Original_TAG = "tag";
    public Map<String,String> tagToVal;
    public Map<String, ColumnIdentifier> tagToIdentifier;

    public TreasConfig(int K)
    {
        for(int r = 0; r<K; r++){
            String newTag = Original_TAG+r;
            String newVal = Original_VAl+r;
            tagToVal.put(newTag,newVal);
            ColumnIdentifier newIdentifier = new ColumnIdentifier(newTag, true);
            tagToIdentifier.put(newTag,newIdentifier);
        }

    }

    public Collection<ColumnIdentifier> returnIdentifiers(){
        return tagToIdentifier.values();
    }



}
