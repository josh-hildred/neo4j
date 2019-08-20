package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordCursorHack;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordNodeCursor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.NodeRecord;

public class NodeCopier extends EntityCopier
{
    private IdSequence ids;
    private NeoStores fromStore;
    private NeoStores toStores;
    private RecordCursorHack cursorFactory;

    NodeCopier( NeoStores fromStore, NeoStores toStores, IdSequence ids, IdSequence propIds )
    {
        super(fromStore, toStores, (FirstFitIdGetter.FirstFitIds) propIds);
        this.fromStore = fromStore;
        this.toStores = toStores;
        this.ids = ids;
        this.cursorFactory = new RecordCursorHack();
    }

    public void copy(long id, RelationshipCopier copier)
    {
        RecordNodeCursor cursor = cursorFactory.getNodeCursor( fromStore.getNodeStore() );
        cursor.single( id );
        cursor.next();
        NodeRecord record = cursor.clone();
        if( !record.isDense() )
        {
            record.setNextRel(copier.copyChain(record.getNextRel(), record.getId() ) );
        }
        record.setNextProp( copyPropertyChain( record.getNextProp() ) );
        record.setId( ids.nextId() );
        toStores.getNodeStore().updateRecord(record);
    }
}
