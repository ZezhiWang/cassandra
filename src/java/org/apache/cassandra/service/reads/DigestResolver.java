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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.Erasure;
import org.apache.cassandra.service.TagVal;
import org.apache.cassandra.service.TreasConsts;
import org.apache.cassandra.service.TreasTag;
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
                while(ri.hasNext())
                {
                    TreasTag tagOne = null;
                    TreasTag tagTwo = null;
                    TreasTag tagThree = null;
                    String valOne = "";
                    String valTwo = "";
                    String valThree = "";
                    for(Cell c : ri.next().cells()) {
                        if (c.column().name.equals(TreasConsts.tagOneIdentifier)) {
                            tagOne = TreasTag.deserialize(c.value());
                            int count = tagCount.containsKey(tagOne) ? tagCount.get(tagOne) : 0;
                            tagCount.put(tagOne, count + 1);
                        } else if (c.column().name.equals(TreasConsts.tagTwoIdentifier)) {
                            tagTwo = TreasTag.deserialize(c.value());
                            int count = tagCount.containsKey(tagTwo) ? tagCount.get(tagTwo) : 0;
                            tagCount.put(tagTwo, count + 1);
                        } else if (c.column().name.equals(TreasConsts.tagThreeIdentifier)) {
                            tagThree = TreasTag.deserialize(c.value());
                            int count = tagCount.containsKey(tagThree) ? tagCount.get(tagThree) : 0;
                            tagCount.put(tagThree, count + 1);
                        } else if (c.column().name.equals(TreasConsts.valOneIdentifier)) {
                            try {
                                valOne = ByteBufferUtil.string(c.value());
                            } catch (CharacterCodingException e) {
                                logger.info("Unable to cast valOne");
                            }
                            if (valOne.equals(""))
                                continue;
                            List<String> valList = valCount.containsKey(tagOne) ? valCount.get(tagOne) : new ArrayList<>();
                            valList.add(valOne);
                            valCount.put(tagOne, valList);
                        } else if (c.column().name.equals(TreasConsts.valTwoIdentifier)) {
                            try {
                                valTwo = ByteBufferUtil.string(c.value());
                            } catch (CharacterCodingException e) {
                                logger.info("Unable to cast valOne");
                            }
                            if (valTwo.equals(""))
                                continue;
                            List<String> valList = valCount.containsKey(tagTwo) ? valCount.get(tagTwo) : new ArrayList<>();
                            valList.add(valTwo);
                            valCount.put(tagTwo, valList);
                        } else if (c.column().name.equals(TreasConsts.valThreeIdentifier)) {
                            try {
                                valThree = ByteBufferUtil.string(c.value());
                            } catch (CharacterCodingException e) {
                                logger.info("Unable to cast valOne");
                            }
                            if (valThree.equals(""))
                                continue;
                            List<String> valList = valCount.containsKey(tagThree) ? valCount.get(tagThree) : new ArrayList<>();
                            valList.add(valThree);
                            valCount.put(tagThree, valList);
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

        ColumnIdentifier tagOneIdentifier = new ColumnIdentifier(TreasConsts.TAG_ONE, true);
        ColumnIdentifier tagTwoIdentifier = new ColumnIdentifier(TreasConsts.TAG_TWO, true);
        ColumnIdentifier tagThreeIdentifier = new ColumnIdentifier(TreasConsts.TAG_THREE, true);

        for (MessageIn<ReadResponse> message : responses) {
            ReadResponse curResponse = message.payload;
            assert !curResponse.isDigestResponse();
            PartitionIterator pi = UnfilteredPartitionIterators.filter(curResponse.makeIterator(command), command.nowInSec());
            while (pi.hasNext()) {
                RowIterator ri = pi.next();
                while (ri.hasNext()) {
                    TreasTag curTag = new TreasTag();
                    for (Cell c : ri.next().cells()) {
                        if (c.column().name.equals(tagOneIdentifier) || c.column().name.equals(tagTwoIdentifier) || c.column().name.equals(tagThreeIdentifier)) {
                            TreasTag tmpTag = TreasTag.deserialize(c.value());
                            curTag = tmpTag.isLarger(curTag) ? tmpTag : curTag;
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
