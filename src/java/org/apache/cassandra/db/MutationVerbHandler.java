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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.treas.TreasConsts;
import org.apache.cassandra.service.treas.TreasTag;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutationVerbHandler implements IVerbHandler<Mutation>
{
    private static final Logger logger = LoggerFactory.getLogger(MutationVerbHandler.class);

    private void reply(int id, InetAddressAndPort replyTo)
    {
        Tracing.trace("Enqueuing response to {}", replyTo);
        MessagingService.instance().sendReply(WriteResponse.createMessage(), id, replyTo);
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    public void doVerb(MessageIn<Mutation> message, int id)  throws IOException
    {
        // Check if there were any forwarding headers in this message
        InetAddressAndPort from = (InetAddressAndPort)message.parameters.get(ParameterType.FORWARD_FROM);
        InetAddressAndPort replyTo;
        if (from == null)
        {
            replyTo = message.from;
            ForwardToContainer forwardTo = (ForwardToContainer)message.parameters.get(ParameterType.FORWARD_TO);
            if (forwardTo != null)
                forwardToLocalNodes(message.payload, message.verb, forwardTo, message.from);
        }
        else
        {
            replyTo = from;
        }

        try
        {
            // first we have to create a read request out of the current mutation
            SinglePartitionReadCommand localRead =
            SinglePartitionReadCommand.fullPartitionRead(
            message.payload.getPartitionUpdates().iterator().next().metadata(),
            FBUtilities.nowInSeconds(),
            message.payload.key()
            );

            // execute the read request locally to obtain the tag of the key
            // and extract tag information from the local read
            TreasTag tagLocal = new TreasTag();
            boolean initializedTags = true;
            TreasTag largestTag = null;
            TreasTag smallestTag = null;
            String nameOfSmallestColumnTag = null;
            String nameOfSmallestColumnVal = null;
            String nameOfLargestColumnTag =null;
            String nameOfLargestColumnVal = null;


            try (ReadExecutionController executionController = localRead.executionController();
                 UnfilteredPartitionIterator iterator = localRead.executeLocally(executionController))
            {
                // first we have to transform it into a PartitionIterator
                PartitionIterator pi = UnfilteredPartitionIterators.filter(iterator, localRead.nowInSec());
                while(pi.hasNext())
                {
                    RowIterator ri = pi.next();
                    while(ri.hasNext())
                    {
                        Row r = ri.next();
                        ColumnMetadata colMetaTagOne = ri.metadata().getColumn(ByteBufferUtil.bytes(TreasConsts.TAG_ONE));
                        ColumnMetadata colMetaTagTwo = ri.metadata().getColumn(ByteBufferUtil.bytes(TreasConsts.TAG_TWO));
                        ColumnMetadata colMetaTagThree = ri.metadata().getColumn(ByteBufferUtil.bytes(TreasConsts.TAG_THREE));
                        Cell cTagOne = r.getCell(colMetaTagOne);
                        Cell cTagTwo = r.getCell(colMetaTagTwo);
                        Cell cTagThree = r.getCell(colMetaTagThree);
                        if (cTagOne == null || cTagTwo==null || cTagThree==null)
                        {
                            logger.info("Tags not initialized ");
                            initializedTags = false;
                        }
                        else{
                            List<TreasTag> treasTags = new ArrayList<>();
                            List<String> tagNames = new ArrayList<>();
                            List<String> valNames = new ArrayList<>();
                            tagNames.add(TreasConsts.TAG_ONE);
                            tagNames.add(TreasConsts.TAG_TWO);
                            tagNames.add(TreasConsts.TAG_THREE);
                            valNames.add(TreasConsts.VAL_ONE);
                            valNames.add(TreasConsts.VAL_TWO);
                            valNames.add(TreasConsts.VAL_THREE);
                            TreasTag tagOne = TreasTag.deserialize(cTagOne.value());
                            TreasTag tagTwo = TreasTag.deserialize(cTagTwo.value());
                            TreasTag tagThree = TreasTag.deserialize(cTagThree.value());
                            treasTags.add(tagOne);
                            treasTags.add(tagTwo);
                            treasTags.add(tagThree);
                            largestTag = treasTags.get(0);
                            smallestTag = treasTags.get(0);
                            Iterator<String> tagNamesIterator = tagNames.iterator();
                            Iterator<String> valNamesIterator = valNames.iterator();
                            for(TreasTag tag: treasTags){
                                String currentTagName = tagNamesIterator.next();
                                String currentValName = valNamesIterator.next();
                                if(tag.isLarger(largestTag)){
                                    nameOfLargestColumnTag = currentTagName;
                                    nameOfLargestColumnVal = currentValName;
                                    largestTag=tag;
                                }
                                if(smallestTag.isLarger(tag)){
                                    smallestTag = tag;
                                    nameOfSmallestColumnTag = currentTagName;
                                    nameOfLargestColumnVal = currentValName;
                                }
                            }
                        }
                    }
                }
            }

            // extract the tag information from the mutation
            TreasTag tagRemote = new TreasTag();
            Row data = message.payload.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
            ByteBuffer writtenValue = null;


            for (Cell c : data.cells())
            {

                if(c.column().name.equals(TreasConsts.ORIGINIAL_TAG_IDENTIFIER))
                {
                    tagRemote = TreasTag.deserialize(c.value());
                    logger.info("recv remote {}", tagRemote.toString());
                }
                else if(c.column().name.equals(TreasConsts.ORIGINIAL_TAG_IDENTIFIER)){
                    writtenValue = c.value();
                }
            }
            if(!initializedTags || tagRemote.isLarger(smallestTag))
            {
                Mutation mutation = message.payload;
                List<PartitionUpdate> partitionUpdates = mutation.getPartitionUpdates().asList();
                for (PartitionUpdate partitionUpdate : partitionUpdates)
                {
                    TableMetadata tableMetadata = partitionUpdate.metadata();
                    Iterator<Row> ri = partitionUpdate.iterator();
                    while (ri.hasNext())
                    {
                        Row r = ri.next();

                        ByteBuffer emptyValue = ByteBufferUtil.bytes("");
                        if (!initializedTags)
                        {
                            ColumnMetadata colMetaTagOne = tableMetadata.getColumn(ByteBufferUtil.bytes(TreasConsts.TAG_ONE));
                            ColumnMetadata colMetaTagTwo = tableMetadata.getColumn(ByteBufferUtil.bytes(TreasConsts.TAG_TWO));
                            ColumnMetadata colMetaTagThree = tableMetadata.getColumn(ByteBufferUtil.bytes(TreasConsts.TAG_THREE));
                            ColumnMetadata colMetaValOne = tableMetadata.getColumn(ByteBufferUtil.bytes(TreasConsts.VAL_ONE));
                            ColumnMetadata colMetaValTwo = tableMetadata.getColumn(ByteBufferUtil.bytes(TreasConsts.VAL_TWO));
                            ColumnMetadata colMetaValThree = tableMetadata.getColumn(ByteBufferUtil.bytes(TreasConsts.VAL_THREE));
                            Cell cTagOne = r.getCell(colMetaTagOne);
                            Cell cTagTwo = r.getCell(colMetaTagTwo);
                            Cell cTagThree = r.getCell(colMetaTagThree);
                            Cell cValOne = r.getCell(colMetaValOne);
                            Cell cValTwo = r.getCell(colMetaValTwo);
                            Cell cValThree = r.getCell(colMetaValThree);
                            ByteBuffer treasTagBytes = ByteBufferUtil.bytes(TreasTag.serializeHelper(new TreasTag()));
                            cTagOne.setValue(ByteBufferUtil.bytes(TreasTag.serializeHelper(tagRemote)));
                            cValOne.setValue(writtenValue);
                            cTagTwo.setValue(treasTagBytes);
                            cValTwo.setValue(emptyValue);
                            cTagThree.setValue(treasTagBytes);
                            cValThree.setValue(emptyValue);
                        }
                        else{
                            ColumnMetadata cMetaTagSmallest = tableMetadata.getColumn(ByteBufferUtil.bytes(nameOfSmallestColumnTag));
                            Cell cTagSmallest = r.getCell(cMetaTagSmallest);
                            ColumnMetadata cMetaValSmallest = tableMetadata.getColumn(ByteBufferUtil.bytes(nameOfSmallestColumnVal));
                            Cell cValSmallest = r.getCell(cMetaValSmallest);
                            if (tagRemote.isLarger(largestTag)){
                                ColumnMetadata cMetaTagLargest = tableMetadata.getColumn(ByteBufferUtil.bytes(nameOfLargestColumnTag));
                                Cell cTagLargest = r.getCell(cMetaTagLargest);
                                cTagLargest.setValue(ByteBufferUtil.bytes(TreasTag.serializeHelper(tagRemote)));
                                ColumnMetadata cMetaValLargest = tableMetadata.getColumn(ByteBufferUtil.bytes(nameOfLargestColumnVal));
                                Cell cValLargest = r.getCell(cMetaValLargest);
                                cValLargest.setValue(writtenValue);
                                cTagSmallest.setValue(ByteBufferUtil.bytes(TreasTag.serializeHelper(largestTag)));
                            }
                            else{
                                cTagSmallest.setValue(ByteBufferUtil.bytes(TreasTag.serializeHelper(tagRemote)));
                            }
                            cValSmallest.setValue(emptyValue);
                        }
                    }
                }

                message.payload.applyFuture().thenAccept(o -> reply(id, replyTo));
            }
            reply(id,replyTo);
//                failed();

        }
        catch (WriteTimeoutException wto)
        {
            failed();
        }
    }

    private static void forwardToLocalNodes(Mutation mutation, MessagingService.Verb verb, ForwardToContainer forwardTo, InetAddressAndPort from) throws IOException
    {
        // tell the recipients who to send their ack to
        MessageOut<Mutation> message = new MessageOut<>(verb, mutation, Mutation.serializer).withParameter(ParameterType.FORWARD_FROM, from);
        Iterator<InetAddressAndPort> iterator = forwardTo.targets.iterator();
        // Send a message to each of the addresses on our Forward List
        for (int i = 0; i < forwardTo.targets.size(); i++)
        {
            InetAddressAndPort address = iterator.next();
            Tracing.trace("Enqueuing forwarded write to {}", address);
            MessagingService.instance().sendOneWay(message, forwardTo.messageIds[i], address);
        }
    }
}
