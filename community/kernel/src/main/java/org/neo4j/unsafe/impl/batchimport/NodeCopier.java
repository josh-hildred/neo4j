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

import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordCursorHack;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordNodeCursor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class NodeCopier extends EntityCopier
{
    private IdSequence ids;
    private NeoStores fromStore;
    private NeoStores toStores;
    private RecordCursorHack cursorFactory;
    private ClusteringNodeCache nodeCache;

    NodeCopier( NeoStores fromStore, NeoStores toStores, IdSequence ids, IdSequence propIds, ClusteringNodeCache nodeCache )
    {
        super( fromStore, toStores, (FirstFitIdGetter.FirstFitIds) propIds );
        this.fromStore = fromStore;
        this.toStores = toStores;
        this.ids = ids;
        this.cursorFactory = new RecordCursorHack();
        this.nodeCache = nodeCache;
    }

    public void copy( long id, RelationshipCopier copier )
    {
        RecordNodeCursor cursor = cursorFactory.getNodeCursor( fromStore.getNodeStore() );
        cursor.single( id );
        cursor.next();
        NodeRecord record = cursor.clone();
        /*if ( !record.isDense() )
        {
            record.setNextRel(copier.copyChain(record.getNextRel(), record.getId() ) );
        }*/
        long newId = ids.nextId();
        nodeCache.putIdMap( (int) id, (int) newId );
        //long propId = copyPropertyChain( record.getNextProp(), newId );
        //record.setNextProp( propId );
        record.setId( newId );
        toStores.getNodeStore().updateRecord( record );
        RecordNodeCursor cursorTest = cursorFactory.getNodeCursor( toStores.getNodeStore() );
        cursorTest.single( newId );
        cursorTest.next();
        boolean failed = false;
        PrintWriter printWriter;
        /*if ( cursorTest.getId() != newId || cursorTest.getNextProp() != propId || !cursor.inUse() )
        {
            try
            {
                FileWriter fileWriter = new FileWriter( "/home/josh/Projects/URA_neo4j/clusterdata.txt", true );
                BufferedWriter bufferedWriter = new BufferedWriter( fileWriter );
                printWriter = new PrintWriter( bufferedWriter );
                printWriter.printf( "new id: %d, cursor id: %d", newId, cursorTest.getId() );
                printWriter.printf( "new prop: %d, cursor prop: %d", propId, cursorTest.getNextProp() );
                printWriter.close();
            }
            catch ( IOException e )
            {
                failed = true;
            }
        } */
        assert cursorTest.getId() == newId;
        assert cursorTest.getNextProp() == cursor.getNextProp();
        assert cursorTest.getNextRel() == cursor.getNextRel();
        assert cursorTest.inUse();
        cursor.close();
        cursorTest.close();
    }
}
