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

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import java.util.Collection;

public class EntityCopier
{
    private PropertyStore toPropertyStore;
    private PropertyStore fromPropertyStore;
    private PropertyKeyTokenStore propertyKeyTokenStore;
    private boolean hasPropertyId;
    private FirstFitIdGetter.FirstFitIds ids;

    EntityCopier( NeoStores fromStores, NeoStores toStores, FirstFitIdGetter.FirstFitIds ids )
    {
        this.toPropertyStore = toStores.getPropertyStore();
        this.fromPropertyStore = fromStores.getPropertyStore();
        this.ids = ids;

    }

    protected long copyPropertyChain( long id, long entityId )
    {
        Collection<PropertyRecord> propChain = fromPropertyStore.getPropertyRecordChain( id );
        PropertyRecord prev = null;
        PropertyRecord current = null;
        long ret = -1;
        for ( PropertyRecord record : propChain )
        {
            long nextId = ids.nextId();
            ret = ret == -1 ? nextId : ret;
            if ( prev != null )
            {
                prev.setNextProp( nextId );
                toPropertyStore.updateRecord( prev );
            }
             // this updates property blocks which might blow up since the prop block store is "a from store"
            prev = current;
            current = record.clone();
            current.setId( ids.nextId() );
            if ( current.isNodeSet() )
            {
                current.setNodeId( entityId );
            }
            else
            {
                current.setRelId( entityId );
            }
            if ( prev != null )
            {
                current.setPrevProp( prev.getId() );
            }
        }
        return ret;
    }

}
