package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordCursorHack;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class RelationshipCopier extends EntityCopier
{
    private IdSequence ids;
    private NeoStores fromStore;
    private NeoStores toStores;
    private RecordCursorHack cursorFactory;

    RelationshipCopier(NeoStores fromStore, NeoStores toStores, IdSequence ids, IdSequence propIds)
    {
        super(fromStore, toStores, (FirstFitIdGetter.FirstFitIds) propIds);
        this.ids = ids;
        this.fromStore = fromStore;
        this.toStores = toStores;
        this.cursorFactory = new RecordCursorHack();
    }

    public long copyChain(long id, long nodeId)
    {
        RecordCursorHack.RecordRelationshipCursorHack relationshipCursor = cursorFactory.getRelationshipCursor( fromStore.getRelationshipStore(),
                fromStore.getRelationshipGroupStore() );
        relationshipCursor.init( nodeId, id );
        long ret = -1;
        while( relationshipCursor.next() )
        {
            long relId = copyRecord( relationshipCursor );
            ret = ret == -1 ? relId : ret;
        }
        return ret;
    }

    private long copyRecord( RecordCursorHack.RecordRelationshipCursorHack cursor )
    {
       RelationshipRecord record = cursor.clone();
       record.setId( ids.nextId() );
       record.setNextProp( copyPropertyChain(cursor.getNextProp() ) );
       toStores.getRelationshipStore().updateRecord(record);
       // need to update the second nodes
       return record.getId();
    }
}
