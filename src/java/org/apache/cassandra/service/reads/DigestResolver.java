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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.SbqConsts;
import org.apache.cassandra.service.TagVal;
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


    public ReadResponse getMaxResponse()
    {
        // check all data responses,
        // extract the one with max z value

        Map<TagVal,Integer> tvCount = new HashMap<>();
        Map<TagVal,ReadResponse> tvResp = new HashMap<>();

        for (MessageIn<ReadResponse> message : responses)
        {
            ReadResponse curResponse = message.payload;
            assert !curResponse.isDigestResponse();
            PartitionIterator pi = UnfilteredPartitionIterators.filter(curResponse.makeIterator(command), command.nowInSec());
            while(pi.hasNext())
            {
                RowIterator ri = pi.next();
                ColumnMetadata tagMetaData = ri.metadata().getColumn(ByteBufferUtil.bytes(SbqConsts.TS));
                ColumnMetadata valMetaData = ri.metadata().getColumn(ByteBufferUtil.bytes(SbqConsts.VAL));
                while(ri.hasNext())
                {
                    Row r = ri.next();
                    TagVal tmpTv = new TagVal(-1,"");
                    Cell tagCell = r.getCell(tagMetaData);
                    Cell valCell = r.getCell(valMetaData);
                    if(tagCell!=null){
                        tmpTv.ts = ByteBufferUtil.toInt(tagCell.value());
                    }
                    try{
                        tmpTv.val = ByteBufferUtil.string(valCell.value());
                    } catch (CharacterCodingException e){
                        logger.info("Err getting value: {}",e);
                    }
                    int count = tvCount.containsKey(tmpTv) ? tvCount.get(tmpTv)+1 : 1;
                    tvCount.put(tmpTv,count);
                    if(!tvResp.containsKey(tmpTv))
                        tvResp.put(tmpTv,curResponse);

                }
            }
        }

        int maxTs = -1;
        ReadResponse maxResponse = null;
        for(TagVal tv : tvCount.keySet()){
            if (tvCount.get(tv) > SbqConsts.F && tv.ts > maxTs){
                maxTs = tv.ts;
                maxResponse = tvResp.get(tv);
            }
        }

        return maxResponse;
    }

    public int getMaxTs() {
        int maxTs = -1;
        for (MessageIn<ReadResponse> message : responses) {
            ReadResponse curResponse = message.payload;
            assert !curResponse.isDigestResponse();
            PartitionIterator pi = UnfilteredPartitionIterators.filter(curResponse.makeIterator(command), command.nowInSec());
            while (pi.hasNext()) {
                RowIterator ri = pi.next();
                ColumnMetadata tagMetaData = ri.metadata().getColumn(ByteBufferUtil.bytes(SbqConsts.TS));

                while (ri.hasNext()) {
                    Row r  = ri.next();
                    Cell tagCell = r.getCell(tagMetaData);

                    int curTs = -1;
                    if(tagCell!=null){
                        curTs = ByteBufferUtil.toInt(tagCell.value());
                    }
                    if (curTs > maxTs) {
                        maxTs = curTs;
                    }
                }
            }
        }
        return maxTs;
    }

    public boolean isDataPresent()
    {
        return dataResponse != null;
    }
}
