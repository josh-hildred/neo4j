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
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class RelationshipCopier extends EntityCopier
{
    private IdSequence ids;
    private NeoStores fromStore;
    private NeoStores toStores;
    private RecordCursorHack cursorFactory;

    RelationshipCopier( NeoStores fromStore, NeoStores toStores, IdSequence ids, IdSequence propIds )
    {
        super( fromStore, toStores, (FirstFitIdGetter.FirstFitIds) propIds );
        this.ids = ids;
        this.fromStore = fromStore;
        this.toStores = toStores;
        this.cursorFactory = new RecordCursorHack();
    }

    public long copyChain( long id, long nodeId )
    {
        RecordCursorHack.RecordRelationshipCursorHack relationshipCursor = cursorFactory.getRelationshipCursor( fromStore.getRelationshipStore(),
                fromStore.getRelationshipGroupStore() );
        relationshipCursor.init( nodeId, id );
        long ret = -1;
        while ( relationshipCursor.next() )
        {
            long relId = copyRecord( relationshipCursor );
            ret = ret == -1 ? relId : ret;
        }
        relationshipCursor.close();
        return ret;
    }

    private long copyRecord( RecordCursorHack.RecordRelationshipCursorHack cursor )
    {
       RelationshipRecord record = cursor.clone();
       long newId = ids.nextId();
       record.setId( newId );
       record.setNextProp( copyPropertyChain( cursor.getNextProp(), newId ) );
       toStores.getRelationshipStore().updateRecord( record );
       // need to update the nodes
       return record.getId();

    }
}
