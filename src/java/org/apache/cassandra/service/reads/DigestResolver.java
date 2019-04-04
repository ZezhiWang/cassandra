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
package org.apache.cassandra.service.reads;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.deser.std.MapEntryDeserializer;
import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.Erasure;
import org.apache.cassandra.service.treas.TagVal;
import org.apache.cassandra.service.treas.TreasConsts;
import org.apache.cassandra.service.treas.TreasTag;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DigestResolver extends ResponseResolver
{
    private volatile ReadResponse dataResponse;

    public DigestResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, ReadRepair readRepair, int maxResponseCount)
    {
        super(keyspace, command, consistency, readRepair, maxResponseCount);
        Preconditions.checkArgument(command instanceof SinglePartitionReadCommand,
                                    "DigestResolver can only be used with SinglePartitionReadCommand commands");
    }

    @Override
    public void preprocess(MessageIn<ReadResponse> message)
    {
        super.preprocess(message);
        if (dataResponse == null && !message.payload.isDigestResponse())
            dataResponse = message.payload;
    }

    // this is the original method, NoopReadRepair has a call to this method
    // simply change the method signature to ReadResponse getData() will raise an compiler error
    public PartitionIterator getData()
    {
        assert isDataPresent();
        return UnfilteredPartitionIterators.filter(dataResponse.makeIterator(command), command.nowInSec());
    }

    // this is a new method for AbstractReadExecutor, which may want to use ReadResponse more than once
    public ReadResponse getReadResponse()
    {
        assert isDataPresent();
        return dataResponse;
    }

    public boolean responsesMatch()
    {
        long start = System.nanoTime();

        // validate digests against each other; return false immediately on mismatch.
        ByteBuffer digest = null;
        for (MessageIn<ReadResponse> message : responses)
        {
            ReadResponse response = message.payload;

            ByteBuffer newDigest = response.digest(command);
            if (digest == null)
                digest = newDigest;
            else if (!digest.equals(newDigest))
                // rely on the fact that only single partition queries use digests
                return false;
        }

        if (logger.isTraceEnabled())
            logger.trace("responsesMatch: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        return true;
    }


    public TagVal getMaxTagVal()
    {
        // check all data responses,
        // extract the one with max z value

        Map<TreasTag,Integer> tagCount = new HashMap<>();
        Map<TreasTag, List<String>> valCount = new HashMap<>();

        for (MessageIn<ReadResponse> message : responses)
        {
            ReadResponse curResponse = message.payload;
            assert !curResponse.isDigestResponse();
            PartitionIterator pi = UnfilteredPartitionIterators.filter(curResponse.makeIterator(command), command.nowInSec());
            while(pi.hasNext())
            {
                RowIterator ri = pi.next();
                Boolean flag = false;
                while(ri.hasNext())
                {
                    Map<String,TreasTag> tagM = new HashMap<>();
                    for(Cell c : ri.next().cells()) {
                        for (Map.Entry<String,ColumnIdentifier> ety : TreasConsts.CONFIG.tagToIdentifier.entrySet()){
                            if (c.column().name.equals(ety.getValue())){
                                String tagKey = ety.getKey();
                                TreasTag tmpTag = TreasTag.deserialize(c.value());
                                tagM.put(tagKey,tmpTag);
                                int count = tagCount.containsKey(tagKey) ? tagCount.get(tagKey) : 0;
                                tagCount.put(tmpTag, count + 1);

                                flag = true;
                                break;
                            }
                        }

                        if (flag){
                            flag = false;
                            continue;
                        }

                        for (Map.Entry<String,ColumnIdentifier> ety : TreasConsts.CONFIG.valToIdentifier.entrySet()){
                            if (c.column().name.equals(ety.getValue())){
                                String tmpVal = "";
                                try {
                                    tmpVal = ByteBufferUtil.string(c.value());
                                } catch (CharacterCodingException e) {
                                    logger.info("Unable to cast valOne");
                                }
                                if (tmpVal.equals(""))
                                    break;
                                TreasTag tmpTag = tagM.get(TreasConsts.CONFIG.valToTag.get(ety.getKey()));
                                List<String> valList = valCount.containsKey(tmpTag) ? valCount.get(tmpTag) : new ArrayList<>();
                                valList.add(tmpVal);
                                valCount.put(tmpTag, valList);
                                break;
                            }
                        }
                    }
                }
            }
        }

        TreasTag maxTagStar = new TreasTag();
        for(TreasTag t : tagCount.keySet()){
            if (tagCount.get(t) >= TreasConsts.K && t.isLarger(maxTagStar))
                maxTagStar = t;
        }

        TreasTag maxTagDec = new TreasTag();
        for(TreasTag t : valCount.keySet()){
            if (valCount.get(t).size() >= TreasConsts.L && t.isLarger(maxTagDec))
                maxTagDec = t;
        }


        String decoded = "";
        if(!maxTagStar.isLarger(maxTagDec))
            decoded = Erasure.decode(valCount.get(maxTagDec));

        return new TagVal(maxTagDec,decoded);
    }

    public TreasTag getMaxTag() {
        TreasTag maxTag = new TreasTag();

        for (MessageIn<ReadResponse> message : responses) {
            ReadResponse curResponse = message.payload;
            assert !curResponse.isDigestResponse();
            PartitionIterator pi = UnfilteredPartitionIterators.filter(curResponse.makeIterator(command), command.nowInSec());
            while (pi.hasNext()) {
                RowIterator ri = pi.next();
                while (ri.hasNext()) {
                    TreasTag curTag = new TreasTag();
                    for (Cell c : ri.next().cells()) {
                        for(ColumnIdentifier ci : TreasConsts.CONFIG.tagIdentifiers()){
                            if(c.column().name.equals(ci)){
                                TreasTag tmpTag = TreasTag.deserialize(c.value());
                                curTag = tmpTag.isLarger(curTag) ? tmpTag : curTag;
                            }
                        }
                    }
                    if (curTag.isLarger(maxTag)) {
                        maxTag = curTag;
                    }
                }
            }
        }
        return maxTag;
    }

    public boolean isDataPresent()
    {
        return dataResponse != null;
    }
}
