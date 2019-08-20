package org.neo4j.unsafe.impl.batchimport;


import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.util.ValueUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

public class EntityCopier
{
    private PropertyStore toPropertyStore;
    private PropertyStore fromPropertyStore;
    private PropertyKeyTokenStore propertyKeyTokenStore;
    private boolean hasPropertyId;
    private FirstFitIdGetter.FirstFitIds ids;

    EntityCopier(NeoStores fromStores, NeoStores toStores, FirstFitIdGetter.FirstFitIds ids)
    {
        this.toPropertyStore = toStores.getPropertyStore();
        this.fromPropertyStore = fromStores.getPropertyStore();
        this.ids = ids;

    }

    protected long copyPropertyChain(long id)
    {
        Collection<PropertyRecord> propChain = fromPropertyStore.getPropertyRecordChain( id );
        PropertyRecord prev = null;
        PropertyRecord current = null;
        long ret = -1;
        for ( PropertyRecord record : propChain)
        {
            long nextId = ids.nextId();
            ret = ret == -1 ? nextId : ret;
            if ( prev != null )
            {
                prev.setNextProp( nextId );
            }
            toPropertyStore.updateRecord(prev); // this updates property blocks which might blow up since the prop block store is "a from store"
            prev = current;
            current = record.clone();
            current.setId(ids.nextId());
            if ( prev != null )
            {
                current.setPrevProp( prev.getId() );
            }
        }
        return ret;
    }

}
