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

import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordCursorHack;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordNodeCursor;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordPropertyCursor;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.ClusteringNeoStores;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;


public class ClusteringLogic implements Closeable
{
    private final ClusteringNeoStores finalNeoStores;
    private final BatchingNeoStores initialNeostores;
    private final RecordCursorHack cursorFactory;

    private ClusteringNodeCache nodeCache;
    private RecordNodeCursor nodeCursor;

    public ClusteringLogic( ClusteringNeoStores finalNeoStores, BatchingNeoStores initialNeostores )
    {
        this.finalNeoStores = finalNeoStores;
        this.initialNeostores = initialNeostores;
        this.cursorFactory = new RecordCursorHack();
    }

    public void initialize()
    {
        nodeCursor = cursorFactory.getNodeCursor(initialNeostores.getNodeStore());
        //singleNodeCursor = cursorFactory.getNodeCursor(initialNeostores.getNodeStore());
        //private RecordNodeCursor singleNodeCursor;
        long nodeHighId = initialNeostores.getNodeStore().getHighId();
        nodeCache = new ClusteringNodeCache( (int) nodeHighId );
    }

    public void CalculateCluster( int ep, int minPts )
    {
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
                RecordNodeCursor singleNodeCursor = cursorFactory.getNodeCursor( initialNeostores.getNodeStore() );
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
        RecordCursorHack.RecordRelationshipCursorHack relationshipCursor = cursorFactory.getRelationshipCursor( initialNeostores.getRelationshipStore(),
                initialNeostores.getRelationshipGroupStore() );
        relationshipCursor.init( Id, relId );
        //printMsg( "in explore neighbors: %d, %d\n",i, j );
        while ( relationshipCursor.next() )
        {
            RecordNodeCursor singleNodeCursor = cursorFactory.getNodeCursor( initialNeostores.getNodeStore() );
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
        RecordNodeCursor nodeCursor = cursorFactory.getNodeCursor( initialNeostores.getNodeStore() );
        nodeCursor.scan();
        RecordCursorHack.RecordPropertyCursorHack propertyCursor = cursorFactory.getPropertyCursor( initialNeostores.getPropertyStore() );
        RecordCursorHack.RecordRelationshipCursorHack relationshipCursor = cursorFactory.getRelationshipCursor(initialNeostores.getRelationshipStore(),
                initialNeostores.getRelationshipGroupStore() );
        while ( nodeCursor.next() )
        {
            propertyCursor.reset();
            propertyCursor.init( nodeCursor.getLabelField() );
            while( propertyCursor.next() )
            {
                nodeCache.incrementNodePropertiesCounts( (int) nodeCursor.getId(), 1);
            }
            if( !nodeCursor.isDense() )
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
        FirstFitIdGetter nodeIdsGetter = new FirstFitIdGetter( finalNeoStores.getNodeStore() );
        FirstFitIdGetter relIdsGetter = new FirstFitIdGetter( finalNeoStores.getRelationshipStore() );
        FirstFitIdGetter relGroupIdsGetter = new FirstFitIdGetter( finalNeoStores.getRelationshipGroupStore() );
        FirstFitIdGetter propIdsGetter = new FirstFitIdGetter( finalNeoStores.getPropertyStore() );
        for ( int i = 2; i < nodeCache.getNumberOfClusters(); i++ )
        {
            int numRelationshipRecords = nodeCache.getClusterRels( i );
            FirstFitIdGetter.FirstFitIds relId = relIdsGetter.firstFit( numRelationshipRecords );
            int numRelationshipGroupRecords = nodeCache.getClusterRelGroups( i );
            //FirstFitIdGetter.FirstFitIds relGroupId = relGroupIdsGetter.firstFit( numRelationshipGroupRecords );
            int numPropertyRecords = nodeCache.getClusterProps( i );
            FirstFitIdGetter.FirstFitIds propId = propIdsGetter.firstFit( numPropertyRecords );
            int numNodesRecords = nodeCache.sizeOfCluster( i );
            FirstFitIdGetter.FirstFitIds newNodeId = nodeIdsGetter.firstFit( numNodesRecords );
            LinkedList nodes = nodeCache.getClusterNodes( i );
            NodeCopier nodeCopier = new NodeCopier( initialNeostores.getNeoStores(), finalNeoStores.getNeoStores(), newNodeId, propId );
            RelationshipCopier relationshipCopier = new RelationshipCopier( initialNeostores.getNeoStores(), finalNeoStores.getNeoStores(), relId, propId );
            while( !nodes.isEmpty() )
            {
                long nodeId = (long) nodes.pop();
                nodeCopier.copy( nodeId, relationshipCopier );
            }
        }
        // fill up rest with noise
        LinkedList noise = nodeCache.getClusterNodes(0 );

        int numRelationshipRecords = nodeCache.getClusterRels( 0 );
        FirstFitIdGetter.FirstFitIds relId = relIdsGetter.firstFit( numRelationshipRecords );
        int numRelationshipGroupRecords = nodeCache.getClusterRelGroups( 0 );
        FirstFitIdGetter.FirstFitIds relGroupId = relGroupIdsGetter.firstFit( numRelationshipGroupRecords );
        int numPropertyRecords = nodeCache.getClusterProps( 0 );
        FirstFitIdGetter.FirstFitIds propId = propIdsGetter.firstFit( numPropertyRecords );
        int numNodesRecords = nodeCache.sizeOfCluster( 0 );
        FirstFitIdGetter.FirstFitIds newNodeId = nodeIdsGetter.firstFit( numNodesRecords );

        NodeCopier nodeCopier = new NodeCopier( initialNeostores.getNeoStores(), finalNeoStores.getNeoStores(), newNodeId, propId );
        RelationshipCopier relationshipCopier = new RelationshipCopier( initialNeostores.getNeoStores(), finalNeoStores.getNeoStores(), relId, propId )
        while ( !noise.isEmpty() )
        {
            nodeCopier.copy( (long) noise.pop(), relationshipCopier );
        }
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
        nodeCursor.close();
    }
}
