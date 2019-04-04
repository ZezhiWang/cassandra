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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.cassandra.service.treas.TreasConfig;
import org.apache.cassandra.service.treas.TreasConsts;
import org.apache.cassandra.service.treas.TreasTag;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

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
            Pair<Boolean,Mutation> booleanMutationPair = createTreasMutation(message.payload);
            if(booleanMutationPair.left){
                booleanMutationPair.right.applyFuture().thenAccept(o -> reply(id, replyTo));
            }

            reply(id,replyTo);
//                failed();
        }
        catch (WriteTimeoutException wto)
        {
            failed();
        }
    }
    public static Pair<Boolean,Mutation> createTreasMutation(Mutation oldMutation){
        TableMetadata tableMetadata = oldMutation.getPartitionUpdates().iterator().next().metadata();
        SinglePartitionReadCommand localRead =
        SinglePartitionReadCommand.fullPartitionRead(
        tableMetadata,
        FBUtilities.nowInSeconds(),
        oldMutation.key()
        );

        boolean initializedTags = false;
        TreasTag largestTag = new TreasTag();
        TreasTag smallestTag = new TreasTag();
        String nameOfSmallestColumnTag = null;
        String nameOfLargestColumnTag =null;


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
                    Map<String,Cell> tagToCell = new HashMap<>();
                    Set<String> tags = TreasConsts.CONFIG.returnTags();
                    for(String tag: tags)
                    {
                        ColumnMetadata colMetaTagOne = ri.metadata().getColumn(ByteBufferUtil.bytes(tag));
                        Cell ctag = r.getCell(colMetaTagOne);
                        if (ctag == null){
                            logger.info("Tag not initialized");
                            break;
                        }
                        else{
                            tagToCell.put(tag,ctag);
                            initializedTags = true;
                        }
                    }if(initializedTags){
                        for(String tagName: TreasConsts.CONFIG.returnTags()){
                            Cell ctag = tagToCell.get(tagName);
                            TreasTag tag = TreasTag.deserialize(ctag.value());
                            if(tag.isLarger(largestTag)){
                                nameOfLargestColumnTag = tagName;
                                largestTag=tag;
                            }
                            if(smallestTag.isLarger(tag)){
                                smallestTag = tag;
                                nameOfSmallestColumnTag = tagName;
                            }
                        }
                    }
                }
            }
        }

        // extract the tag information from the mutation
        TreasTag tagRemote = new TreasTag();
        Row data = oldMutation.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
        ByteBuffer writtenValue = null;

        for (Cell c : data.cells())
        {
            if(c.column().name.equals(TreasConfig.CI_TAG)){
                tagRemote = TreasTag.deserialize(c.value());
                logger.info("recv remote {}", tagRemote.toString());
            } else if(c.column().name.equals(TreasConfig.CI_VAL)){
                writtenValue = c.value();
            }
        }
        ByteBuffer emptyValue = ByteBufferUtil.bytes("");
        String emptyTagString = TreasTag.serializeHelper(new TreasTag());
        String remoteTagString = TreasTag.serializeHelper(tagRemote);
        String largestTagString = TreasTag.serializeHelper(largestTag);

        Mutation.SimpleBuilder mutationBuilder = Mutation.simpleBuilder(oldMutation.getKeyspaceName(), oldMutation.key());

        long timeStamp = FBUtilities.timestampMicros();
        Row.SimpleBuilder rowBuilder = mutationBuilder.update(tableMetadata)
                                                      .timestamp(timeStamp)
                                                      .row();
        if(!initializedTags || tagRemote.isLarger(smallestTag))
        {
            List<PartitionUpdate> partitionUpdates = oldMutation.getPartitionUpdates().asList();
            for (PartitionUpdate partitionUpdate : partitionUpdates)
            {
                Iterator<Row> ri = partitionUpdate.iterator();
                while (ri.hasNext())
                {
                    Row r = ri.next();

                    if (!initializedTags)
                    {
                        boolean firstValue = true;
                        for (Map.Entry<String, String> pair : TreasConsts.CONFIG.getTagToIdValSet())
                        {
                            String tagName = pair.getKey();
                            String valueName = pair.getValue();
                            if (firstValue)
                            {
                                rowBuilder.add(tagName, remoteTagString);
                                rowBuilder.add(valueName, writtenValue);
                                firstValue = false;
                            }
                            else
                            {
                                rowBuilder.add(tagName, emptyTagString);
                                rowBuilder.add(valueName, emptyValue);
                            }
                        }
                    }
                    else
                    {
//                        String nameOfSmallestColumnVal = TreasConsts.CONFIG.getVal(nameOfSmallestColumnTag);
                        String nameOfLargestColumnVal = TreasConsts.CONFIG.getVal(nameOfLargestColumnTag);
                        if (tagRemote.isLarger(largestTag))
                        {
                            rowBuilder.add(nameOfLargestColumnTag, remoteTagString);
                            rowBuilder.add(nameOfLargestColumnVal, writtenValue);
                            rowBuilder.add(nameOfSmallestColumnTag, largestTagString);
                        }
                        else
                        {
                            rowBuilder.add(nameOfSmallestColumnTag, remoteTagString);
                        }
//                        rowBuilder.add(nameOfSmallestColumnVal, emptyValue);
                    }
                }
            }
            Mutation newMutation = mutationBuilder.build();
            return new Pair<>(true, newMutation);
        }
        return new Pair<>(false,null);
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
