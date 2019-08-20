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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.Vector;

public class ClusteringNodeCache implements AutoCloseable
{
    private int[] nodeLabels;
    private int[] iArr;
    private int[] jArr;
    private LinkedList<Integer>[] clusterData;
    private int[] clusterSizes;
    private MergingQueue queue;
    private int highClusterId;
    private int size;
    private int[] relationshipsCounts;
    private int[] relationshipsPropertyCounts;
    private int[] nodePropertyCounts;
    private int[] relationshipGroupsCounts;
    private int[] clusterRelationshipCounts;
    private int[] clusterRelationshipsPropertiesCounts;
    private int[] clusterRelationshipGroupsCounts;
    private int[] clusterNodePropertiesCounts;

    ClusteringNodeCache( int size )
    {
        this.size = size;
        nodeLabels = new int[size];
        iArr = new int[size];
        jArr = new int[size];
        this.relationshipGroupsCounts = new int[size];
        this.relationshipsCounts = new int[size];
        this.nodePropertyCounts = new int[size];
        this.relationshipsPropertyCounts = new int[size];
        this.queue = new MergingQueue();
        this.highClusterId = 0;
    }

    public int getLabel( long id )
    {
        return nodeLabels[ (int) id];
    }

    void setLabel( long id, int label )
    {
        nodeLabels[(int) id] = label;
    }

    int nextClusterId()
    {
        highClusterId++;
        return highClusterId;
    }

    public void incrementRelationshipsCounts(int i, int n)
    {
        relationshipsCounts[i - 1] += n;
    }

    public void incrementRelationshipsGroupsCounts(int i, int n)
    {
        relationshipGroupsCounts[i - 1] += n;
    }

    public void incrementRelationshipPropertiesCounts(int i, int n)
    {
        relationshipsPropertyCounts[i - 1] += n;
    }

    public void incrementNodePropertiesCounts( int i, int n)
    {
        nodePropertyCounts[i - 1] += n;
    }

    class MergingQueue
    {
        private LinkedList ll;

        MergingQueue()
        {
            this.ll = new LinkedList();
        }

        public long pop()
        {
            return (long) ll.pop();
        }

        public void push( long id )
        {
            ll.push(id);
        }

        LinkedList getLL()
        {
            return ll;
        }
        public void merge( MergingQueue q )
        {
            ll.addAll(q.getLL());
        }
        public int size()
        {
            return ll.size();
        }
    }

    MergingQueue getNewQueue()
    {
        return new MergingQueue();
    }

    void mergeQueues( MergingQueue q )
    {
        queue.merge(q);
    }

    public MergingQueue getQueue()
    {
        return queue;
    }
    public boolean visited( long nodeId, int i, int j )
    {
        if ( iArr[(int)nodeId] < i ||  (iArr[(int)nodeId] == i && jArr[(int) nodeId] < j) )
        {
            iArr[(int) nodeId] = i;
            jArr[(int) nodeId] = j;
            return false;
        }
        return true;
    }

    public void calculateClusteringData()
    {
        clusterSizes = new int [highClusterId + 2];
        clusterRelationshipCounts = new int [highClusterId + 2];
        clusterRelationshipsPropertiesCounts =  new int [highClusterId + 2];
        clusterRelationshipGroupsCounts = new int [highClusterId + 2];
        clusterNodePropertiesCounts = new int [highClusterId + 2];
        clusterData = new LinkedList[highClusterId+2];
        for ( int i = 0 ; i < size; i++ )
        {
            clusterNodePropertiesCounts[nodeLabels[i] + 1] = nodePropertyCounts[i];
            clusterRelationshipCounts[nodeLabels[i] + 1] = relationshipsCounts[i];
            clusterRelationshipsPropertiesCounts[nodeLabels[i] + 1] = relationshipsPropertyCounts[i];
            clusterSizes[nodeLabels[i] + 1]++;
            clusterData[nodeLabels[i] + 1].push(i);
        }
    }

    public int getNumberOfClusters()
    {
        return highClusterId + 2;
    }

    public int sizeOfCluster(int id)
    {
        return clusterSizes[id];
    }

    public LinkedList getClusterNodes(int id)
    {
        return clusterData[id];
    }

    void printClusterData()
    {
        boolean failed = false;
        PrintWriter printWriter;
        try
        {
            FileWriter fileWriter = new FileWriter( "/home/josh/Projects/URA_neo4j/clusterdata.txt", true );
            BufferedWriter bufferedWriter = new BufferedWriter( fileWriter );
            printWriter = new PrintWriter( bufferedWriter );
            for ( int i = -1; i < highClusterId + 1; i++ )
            {
                printWriter.printf( "ClusterId ID %d\n Size of Cluster %d\n", i, clusterSizes[i + 1] );
            }
            printWriter.close();
        }
        catch ( IOException e )
        {
            failed = true;
        }
    }
    @Override
    public void close() throws Exception
    {

    }
}
