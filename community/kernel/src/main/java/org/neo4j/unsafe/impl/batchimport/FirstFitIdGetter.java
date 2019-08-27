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

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

import java.util.Vector;

public class FirstFitIdGetter
{
    private int numBins;
    private Vector<Integer> binSizes;
    private Vector<BatchingIdGetter> binIds;
    private int pageSize;
    private final RecordStore<? extends AbstractBaseRecord> source;

    public FirstFitIdGetter( RecordStore<? extends AbstractBaseRecord> source )
    {
        this.numBins = 0;
        this.binSizes = new Vector<>();
        this.binIds = new Vector<>();
        this.source = source;
        this.pageSize = source.getRecordsPerPage() * source.getRecordSize();
    }

    public FirstFitIds firstFit( int size )
    {
        FirstFitIds ret = new FirstFitIds();
        while ( size > pageSize )
        {
            numBins++;
            binSizes.add( pageSize );
            binIds.add( new BatchingIdGetter( source ) );
            ret.addIdGetter( binIds.get(numBins - 1 ) );
            size -= pageSize;
        }
        for ( int i = 0; i < numBins; i++ )
        {
            if ( size + binSizes.get(i) <= pageSize )
            {
                binSizes.setElementAt( binSizes.get( i ) + size, i );
                ret.addIdGetter( binIds.get( i ) );
                return ret;
            }
        }
        numBins++;
        binSizes.add( size );
        binIds.add( new BatchingIdGetter( source ) );
        ret.addIdGetter( binIds.get( numBins - 1 ) );
        return ret;
    }

    public FirstFitIds rest( int size )
    {
        FirstFitIds ret = new FirstFitIds();
        for ( int i = 0; i < numBins; i++ )
        {
            int available = binSizes.get( i );
            if ( available >= size )
            {
                binSizes.setElementAt(binSizes.get( i ) + available - size, i);
                ret.addIdGetter( binIds.get( i ) );
                break;
            }
            binSizes.setElementAt( pageSize, i );
            ret.addIdGetter( binIds.get( i ) );
            size = size - available;
        }
        return ret;
    }

    public class FirstFitIds implements IdSequence
    {
        private Vector<BatchingIdGetter> idGetters;
        private int nextSeq;
        FirstFitIds()
        {
            this.idGetters = new Vector<>();
        }

        void addIdGetter( BatchingIdGetter id )
        {
            idGetters.add( id );
        }
        @Override
        public long nextId()
        {
            while ( !idGetters.get( nextSeq ).fetchNext() )
            {
                nextSeq++;
            }
            return idGetters.get( nextSeq ).nextId();
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            return null;
        }
    }

}
