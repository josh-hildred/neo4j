/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordCursorHack;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordNodeCursor;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.logging.internal.LogService;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.ClusteringNeoStores;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;

import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.store.NodeLabelsField.get;


public class ClusteringLogic implements Closeable
{
    private final ClusteringNeoStores neoStores;
    private NeoStores initialNeoStores;
    private NeoStores finalNeoStores;
    private final RecordCursorHack cursorFactory;

    private ClusteringNodeCache nodeCache;
    private RecordNodeCursor nodeCursor;

    public ClusteringLogic( ClusteringNeoStores neoStores )
    {
        this.neoStores = neoStores;
        this.cursorFactory = new RecordCursorHack();
    }

    public void initialize()
    {
        finalNeoStores = neoStores.getToNeoStores();
        initialNeoStores = neoStores.getFromNeoStores();
        if ( finalNeoStores == null )
        {
            printMsg( "finalNeoStores is null", 0, 0 );
        }
        if ( initialNeoStores == null )
        {
            printMsg( "initialNeoStores is null", 0, 0 );
        }
        if ( initialNeoStores.getNodeStore() == null )
        {
            printMsg( "initialNeoStores.getNodeStore() is null", 0, 0 );
        }
        nodeCursor = cursorFactory.getNodeCursor( initialNeoStores.getNodeStore() );
        //singleNodeCursor = cursorFactory.getNodeCursor(initialNeostores.getNodeStore());
        //private RecordNodeCursor singleNodeCursor;
        long nodeHighId = initialNeoStores.getNodeStore().getHighId();
        nodeCache = new ClusteringNodeCache( (int) nodeHighId );
    }

    public void CalculateCluster( int ep, int minPts )
    {
        printMsg( "in calculate cluster", 0, 0 );
        nodeCursor.scan();
        int i = 0;
        while ( nodeCursor.next() )
        {
            i++;
            //printMsg( "Calculate cluster loop: %d\n",i, 0 );
            long id = nodeCursor.getId();
            if ( nodeCache.getLabel(id) != 0 )
            {
                continue;
            }
            ClusteringNodeCache.MergingQueue nq = exploreNeighbors( id, nodeCursor.allRelationshipsReference(), 0, ep, i, 0 );
            //printMsg( "size of returned queue: %d\n", nq.size(), 0 );
            if ( nq.size() < minPts )
            {
                nodeCache.setLabel( id, -1 );
                continue;
            }
            int c = nodeCache.nextClusterId();
            nodeCache.setLabel( id, c );
            nodeCache.mergeQueues( nq );
            while ( nodeCache.getQueue().size() != 0 )
            {
                long nid = nodeCache.getQueue().pop();
                if ( nodeCache.getLabel(nid) == -1 || nodeCache.getLabel(nid) == 0 )
                {
                    nodeCache.setLabel( nid, c );
                }
                else
                {
                    continue;
                }
                i++;
                RecordNodeCursor singleNodeCursor = cursorFactory.getNodeCursor( initialNeoStores.getNodeStore() );
                singleNodeCursor.single( nid );
                ClusteringNodeCache.MergingQueue rq = exploreNeighbors( nid, singleNodeCursor.allRelationshipsReference(), 0, ep, i, 0 );
                if ( rq.size() >= minPts )
                {
                    nodeCache.mergeQueues( rq );
                }
                singleNodeCursor.close();
            }
        }

    }

    private ClusteringNodeCache.MergingQueue exploreNeighbors( long Id, long relId,  int ep0, int ep, int i, int j )
    {
        ClusteringNodeCache.MergingQueue nq = nodeCache.getNewQueue();
        if ( ep0 > ep )
        {
            return nq;
        }
        RecordCursorHack.RecordRelationshipCursorHack relationshipCursor = cursorFactory.getRelationshipCursor( initialNeoStores.getRelationshipStore(),
                initialNeoStores.getRelationshipGroupStore() );
        relationshipCursor.init( Id, relId );
        //printMsg( "in explore neighbors: %d, %d\n",i, j );
        while ( relationshipCursor.next() )
        {
            RecordNodeCursor singleNodeCursor = cursorFactory.getNodeCursor( initialNeoStores.getNodeStore() );
            long nodeId = relationshipCursor.getFirstNode() == Id ? relationshipCursor.getSecondNode() : relationshipCursor.getFirstNode();
            if ( nodeCache.visited(nodeId, i, j) )
            {
                continue;
            }
            nq.push( nodeId );
            singleNodeCursor.single( nodeId );
            singleNodeCursor.next();
            //printMsg("nodeId From node cursor: %d\nqueue size %d\n", (int) singleNodeCursor.getId(), nq.size() );
            nq.merge( exploreNeighbors( (int) singleNodeCursor.getId(), singleNodeCursor.allRelationshipsReference(), ep0 + 1, ep, i, j + 1 ) );
            singleNodeCursor.close();
        }
        relationshipCursor.close();
        return nq;
    }

    // this should really be done when initial import is done but for simplicity(with a performance hit ) do it here
    public void calculateCounts()
    {
        printMsg( "in calculate counts", 0, 0 );
        RecordNodeCursor nodeCursor = cursorFactory.getNodeCursor( initialNeoStores.getNodeStore() );
        nodeCursor.scan();
        RecordCursorHack.RecordPropertyCursorHack propertyCursor = cursorFactory.getPropertyCursor( initialNeoStores.getPropertyStore() );
        RecordCursorHack.RecordRelationshipCursorHack relationshipCursor = cursorFactory.getRelationshipCursor(initialNeoStores.getRelationshipStore(),
                initialNeoStores.getRelationshipGroupStore() );
        while ( nodeCursor.next() )
        {
            assert nodeCursor.inUse();
            propertyCursor.reset();
            propertyCursor.init( nodeCursor.getNextProp() );
            while ( propertyCursor.next() )
            {
                nodeCache.incrementNodePropertiesCounts( (int) nodeCursor.getId(), 1);
            }
            if ( !nodeCursor.isDense() )
            {
                relationshipCursor.reset();
                relationshipCursor.init(nodeCursor.getId(), nodeCursor.getNextRel());
                while ( relationshipCursor.next() )
                {
                    nodeCache.incrementRelationshipsCounts( (int) nodeCursor.getId(), 1);
                    propertyCursor.reset();
                    propertyCursor.init( relationshipCursor.getNextProp() );
                    while ( propertyCursor.next() )
                    {
                        nodeCache.incrementRelationshipPropertiesCounts((int) nodeCursor.getId(), 1);
                    }
                }
            }
        }
        nodeCache.calculateClusteringData();
    }

    public void writeToStores()
    {
        printMsg( "in write to stores", 0, 0 );
        FirstFitIdGetter nodeIdsGetter = new FirstFitIdGetter( finalNeoStores.getNodeStore() );
        FirstFitIdGetter relIdsGetter = new FirstFitIdGetter( finalNeoStores.getRelationshipStore() );
        FirstFitIdGetter relGroupIdsGetter = new FirstFitIdGetter( finalNeoStores.getRelationshipGroupStore() );
        FirstFitIdGetter propIdsGetter = new FirstFitIdGetter( finalNeoStores.getPropertyStore() );
        for ( int i = 2; i < nodeCache.getNumberOfClusters(); i++ )
        {
            //int numRelationshipRecords = nodeCache.getRelationshipCounts( i );
            FirstFitIdGetter.FirstFitIds relId =  null;//relIdsGetter.firstFit( numRelationshipRecords );
            //int numRelationshipGroupRecords = nodeCache.getRelationshipGroupCount( i );
            //FirstFitIdGetter.FirstFitIds relGroupId = relGroupIdsGetter.firstFit( numRelationshipGroupRecords );
            //int numPropertyRecords = nodeCache.getNodePropertyCount( i );
            FirstFitIdGetter.FirstFitIds propId = null; //propIdsGetter.firstFit( numPropertyRecords );
            int numNodesRecords = nodeCache.sizeOfCluster( i );
            FirstFitIdGetter.FirstFitIds newNodeId = nodeIdsGetter.firstFit( numNodesRecords );
            LinkedList nodes = nodeCache.getClusterNodes( i );
            NodeCopier nodeCopier = new NodeCopier( initialNeoStores, finalNeoStores, newNodeId, propId, nodeCache );
            RelationshipCopier relationshipCopier = new RelationshipCopier( initialNeoStores, finalNeoStores, relId, propId );
            //printMsg( "writing nodes to store for cluster %d ", i, 0 );
            if ( nodes == null )
            {
                continue;
            }
            while ( !nodes.isEmpty() )
            {
                long nodeId = (long) nodes.pop();
                nodeCopier.copy( nodeId, relationshipCopier );
            }
            //printMsg( "done for cluster %d\n", i, 0 );
        }
        // fill up rest with noise
        LinkedList noise = nodeCache.getClusterNodes(0 );

        int numRelationshipRecords = nodeCache.getRelationshipCounts( 0 );
        FirstFitIdGetter.FirstFitIds relId = null;//relIdsGetter.rest( numRelationshipRecords );
        //int numRelationshipGroupRecords = nodeCache.getRelationshipGroupCounts( 0 );
        //FirstFitIdGetter.FirstFitIds relGroupId = relGroupIdsGetter.firstFit( numRelationshipGroupRecords );
        //int numPropertyRecords = nodeCache.getNodePropertyCount( 0 ) + nodeCache.getRelationshipPropertyCounts( 0 );
        FirstFitIdGetter.FirstFitIds propId = null;//= propIdsGetter.rest( numPropertyRecords );
        int numNodesRecords = nodeCache.sizeOfCluster( 0 );
        FirstFitIdGetter.FirstFitIds newNodeId = nodeIdsGetter.rest( numNodesRecords );
        printMsg( "writing noise", 0, 0 );
        NodeCopier nodeCopier = new NodeCopier( initialNeoStores, finalNeoStores, newNodeId, propId, nodeCache );
        RelationshipCopier relationshipCopier = new RelationshipCopier( initialNeoStores, finalNeoStores, relId, propId );
        if ( noise != null )
        {
            while ( !noise.isEmpty() )
            {
                nodeCopier.copy((long) noise.pop(), relationshipCopier);
            }
        }
        updateRecordPointers();
        /*try
        {
            neoStores.getLabelScanStore().init();
        }
        catch ( IOException e )
        {
            assert false;
        }*/
    }

    private void updateRecordPointers()
    {
        printMsg( "in update record pointers ", 0, 0 );
        RecordCursorHack.RecordRelationshipScanCursorHack relationshipCursor =
                cursorFactory.getRelationshipScanCursor( finalNeoStores.getRelationshipStore() );
        relationshipCursor.scan();
        while ( relationshipCursor.next() )
        {
            long firstNode = nodeCache.getIdMap( (int) relationshipCursor.getFirstNode() );
            long secondNode = nodeCache.getIdMap( (int) relationshipCursor.getSecondNode() );
            relationshipCursor.setFirstNode( firstNode );
            relationshipCursor.setSecondNode( secondNode );
            finalNeoStores.getRelationshipStore().updateRecord( relationshipCursor );
        }
        RelationshipGroupRecord groupRecord = finalNeoStores.getRelationshipGroupStore().newRecord();
        long highId = finalNeoStores.getRelationshipGroupStore().getHighId();
        for ( long i = 0; i  <= highId; i++ )
        {
            try
            {
                finalNeoStores.getRelationshipGroupStore().getRecord( i, groupRecord, RecordLoad.NORMAL );
            }
            catch ( InvalidRecordException e )
            {
                groupRecord.clear();
                continue;
            }
            long newId  = nodeCache.getIdMap( (int) groupRecord.getOwningNode() );
            groupRecord.setOwningNode( newId );
            finalNeoStores.getRelationshipGroupStore().updateRecord( groupRecord );
            groupRecord.clear();
        }
        /*RecordNodeCursor cursor = cursorFactory.getNodeCursor( finalNeoStores.getNodeStore() );
        cursor.scan();
        while ( cursor.next( ) )
        {
            /*
            RecordCursorHack.RecordRelationshipCursorHack relationshipCursor = cursorFactory.getRelationshipCursor(
                    finalNeoStores.getRelationshipStore(), finalNeoStores.getRelationshipGroupStore() );
            long nodeId = nodeCursor.isDense() ?
                    finalNeoStores.getRelationshipGroupStore().getRecord( nodeCursor.getNextRel(),
                            finalNeoStores.getRelationshipGroupStore().newRecord(), RecordLoad.FORCE).getId() :
                    finalNeoStores.getRelationshipStore().getRecord( nodeCursor.getNextRel(),
                            finalNeoStores.getRelationshipStore().newRecord(), RecordLoad.FORCE).getId();
            relationshipCursor.init( nodeId, nodeCursor.getNextRel());
            while ( relationshipCursor.next() )
            {
                long firstNode =  nodeCache.getIdMap( (int) relationshipCursor.getFirstNode() );
                long secondNode = nodeCache.getIdMap( (int) relationshipCursor.getSecondNode() );
                relationshipCursor.setFirstNode( firstNode );
                relationshipCursor.setSecondNode( secondNode );
                finalNeoStores.getRelationshipStore().updateRecord( relationshipCursor );
            }
            if ( nodeCursor.isDense() )
            {
                long next = nodeCursor.getNextRel();
                RelationshipGroupRecord groupRecord = finalNeoStores.getRelationshipGroupStore().newRecord();
                while ( true )
                {
                    finalNeoStores.getRelationshipGroupStore().getRecord( next, groupRecord, RecordLoad.FORCE );
                    if ( !groupRecord.inUse() )
                    {
                        break;
                    }
                    groupRecord.setOwningNode( nodeCursor.getId() );
                    finalNeoStores.getRelationshipGroupStore().updateRecord( groupRecord );
                    next = groupRecord.getNext();
                    groupRecord.clear();
                }
            }
            // relationshipCursor.close();
        }
        cursor.close();
    */
        relationshipCursor.close();
    }

    public void buildLabelScanStore() throws IOException
    {
        LabelScanWriter writer = neoStores.getLabelScanStore().newWriter();
        RecordNodeCursor node = cursorFactory.getNodeCursor( neoStores.getToNodeStore() );
        node.scan();
        while ( node.next() )
        {
            if ( node.inUse() )
            {
                writer.write( labelChanges( node.getId(), EMPTY_LONG_ARRAY, get( node, neoStores.getToNodeStore() ) ) );
            }
        }
        node.close();
        writer.close();
    }

    public void buildCountsStore()
    {
        int highLabelId = (int) neoStores.getToNeoStores().getLabelTokenStore().getHighId();
        int anyLabel = highLabelId;
        // Instantiate with high id + 1 since we need that extra slot for the ANY count
        long []labelCounts = new long[highLabelId + 1];
        CountsAccessor.Updater counts = neoStores.getToNeoStores().getCounts().reset(
                neoStores.getToNeoStores().getMetaDataStore().getLastCommittedTransactionId() );

        RecordNodeCursor node = cursorFactory.getNodeCursor( neoStores.getToNodeStore() );
        node.scan();
        while ( node.next() )
        {
            long[] labels = NodeLabelsField.get( node, neoStores.getToNodeStore() );
            if ( labels.length > 0 )
            {
                for ( long labelId : labels )
                {
                    labelCounts[(int) labelId]++;
                }
                //cache.put( node.getId(), labels );
            }
            labelCounts[anyLabel]++;

        }
        for ( int i = 0; i < labelCounts.length; i++ )
        {
                counts.incrementNodeCount( i == anyLabel ? StatementConstants.ANY_LABEL : i, labelCounts[i] );
        }
    }


    private boolean copyUnchangedStores()
    {
        printMsg("in copy unchanged stores", 0, 0);
        try
        {
            FileUtils.copyFile(initialNeoStores.getLabelTokenStore().getStorageFile(),
                    finalNeoStores.getLabelTokenStore().getStorageFile());
            FileUtils.copyFile(initialNeoStores.getPropertyKeyTokenStore().getStorageFile(),
                    finalNeoStores.getPropertyKeyTokenStore().getStorageFile());
            FileUtils.copyFile(initialNeoStores.getMetaDataStore().getStorageFile(),
                    finalNeoStores.getMetaDataStore().getStorageFile());
            FileUtils.copyFile(initialNeoStores.getRelationshipGroupStore().getStorageFile(),
                    finalNeoStores.getRelationshipGroupStore().getStorageFile());
            FileUtils.copyFile(initialNeoStores.getRelationshipTypeTokenStore().getStorageFile(),
                    finalNeoStores.getRelationshipTypeTokenStore().getStorageFile());
        }
        catch ( IOException e )
        {
            return false;
        }
        return true;
    }

    public void printClusterData()
    {
        nodeCache.printClusterData();
    }

    public void printMsg( String msg, int i, int j )
    {
        boolean failed = false;
        PrintWriter printWriter;
        try
        {
            FileWriter fileWriter = new FileWriter( "/home/josh/Projects/URA_neo4j/clusterdata.txt", true );
            BufferedWriter bufferedWriter = new BufferedWriter( fileWriter );
            printWriter = new PrintWriter( bufferedWriter );
            printWriter.printf( msg, i, j );
            printWriter.close();
        }
        catch ( IOException e )
        {
            failed = true;
        }
    }

    @Override
    public void close() throws IOException
    {

    }
}
