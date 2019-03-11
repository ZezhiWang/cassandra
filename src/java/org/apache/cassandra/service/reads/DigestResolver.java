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
import java.nio.file.LinkOption;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.generic.Config;
import org.apache.cassandra.service.generic.LocalCache;
import org.apache.cassandra.service.generic.ValueTimestamp;
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

    public ReadResponse getReadResponse()
    {
        assert isDataPresent();
        return dataResponse;
    }

    public PartitionIterator getData()
    {
        assert isDataPresent();
        return UnfilteredPartitionIterators.filter(dataResponse.makeIterator(command), command.nowInSec());
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

    public ReadResponse extractMaxZResponse()
    {
        // check all data responses,
        // extract the one with max z value
        int maxZ = -1;
        String writerId = "";
        String primaryKey= "";
        ReadResponse maxZResponse = null;

        for (MessageIn<ReadResponse> message : responses)
        {
            ReadResponse response = message.payload;
            // check if the response is indeed a data response
            // we shouldn't get a digest response here
            assert response.isDigestResponse() == false;

            // get the partition iterator corresponding to the
            // current data response
            PartitionIterator pi = UnfilteredPartitionIterators.filter(response.makeIterator(command), command.nowInSec());
            while(pi.hasNext())
            {
                RowIterator ri = pi.next();
                TableMetadata tableMetadata = ri.metadata();
                ColumnMetadata writerIdMetaData = tableMetadata.getColumn(ByteBufferUtil.bytes("writer_id"));
                ColumnMetadata zValueMetaData = tableMetadata.getColumn(ByteBufferUtil.bytes("z_value"));
                while(ri.hasNext())
                {
                    Row r = ri.next();
                    int currentZ = ByteBufferUtil.toInt(r.getCell(zValueMetaData).value());
                    if(currentZ > maxZ)
                    {
                        maxZ = currentZ;
                        maxZResponse = response;
                    } else if (currentZ == maxZ && Config.ID_ON){
                        String curWriter = "";
                        logger.info("Using writer id as the current z equals to maximum z");
                        try{
                            curWriter = ByteBufferUtil.string(r.getCell(writerIdMetaData).value());
                        }
                        catch (CharacterCodingException e){
                            logger.error("cannot cast to string");
                        }
                        maxZResponse = curWriter.compareTo(writerId)>0 ? response : maxZResponse;
                    }
                }

            }
        }
        if(Config.LC_ON){
            updateMaxResponseAndLC(maxZResponse,maxZ);
        }
        return maxZResponse;
    }

    public void updateMaxResponseAndLC(ReadResponse maxZResponse,int maxZ) {
        PartitionIterator pi = UnfilteredPartitionIterators
                .filter(maxZResponse.makeIterator(command), command.nowInSec());
        String primaryKey =" ";
        int dataValue =0;
        while (pi.hasNext()) {
            RowIterator ri = pi.next();
            while (ri.hasNext()) {
                Row r = ri.next();
                for (Cell c : r.cells()) {
                    // todo: the entire row is read for the sake of developmen future improvement could be made
                     if (c.column().isPrimaryKeyColumn()) {
                        try {
                            primaryKey = ByteBufferUtil.string(c.value());
                        } catch (CharacterCodingException e) {
                            logger.error("Could not parse the primary key");
                        }
                    }
                    else if (c.column().name.equals(LocalCache.DATA_IDENTIFIER)) {
                        dataValue = ByteBufferUtil.toInt(c.value());
                    }
                }
            }
        }
        ValueTimestamp valueTimestamp = LocalCache.getVTS(primaryKey);
        if(valueTimestamp ==null || valueTimestamp.getTs()<maxZ){
            logger.info("Updating the local cache with the maximum z value response");
            ValueTimestamp newVTS = new ValueTimestamp(dataValue,maxZ);
            LocalCache.setVTS(primaryKey,newVTS);
        }
        else{
            logger.info("Using the value in the local cache for the response");
            maxZResponse.setVts(valueTimestamp);

        }



    }
    public boolean isDataPresent()
    {
        return dataResponse != null;
    }
}
