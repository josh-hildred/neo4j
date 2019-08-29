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
package org.neo4j.unsafe.impl.batchimport.store;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.store.io.IoTracer;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.function.Predicate;

import static java.lang.String.valueOf;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.io.pagecache.IOLimiter.UNLIMITED;
import static org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore.getLabelScanStoreFile;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

public class ClusteringNeoStores implements AutoCloseable, MemoryStatsVisitor.Visitable
{
    private final FileSystemAbstraction fileSystem;
    private final LogProvider logProvider;
    private final DatabaseLayout toDatabaseLayout;
    private final DatabaseLayout fromDatabaseLayout;
    private final Config neo4jConfig;
    private final Configuration importConfiguration;
    private final PageCache pageCache;
    private final IoTracer ioTracer;
    private final AdditionalInitialIds initialIds;
    private final RecordFormats recordFormats;
    private final boolean externalPageCache;
    private final IdGeneratorFactory idGeneratorFactory;

    private NeoStores toNeoStores;
    private NeoStores fromNeoStores;
    private LifeSupport life = new LifeSupport();
    private PageCacheFlusher flusher;

    private boolean successful;

    private ClusteringNeoStores( FileSystemAbstraction fileSystem, PageCache pageCache, File toDatabaseDirectory, File fromDatabaseDirectory,
                                RecordFormats recordFormats, Config neo4jConfig, Configuration importConfiguration, LogService logService,
                                AdditionalInitialIds initialIds, boolean externalPageCache, IoTracer ioTracer )
    {
        this.fileSystem = fileSystem;
        this.recordFormats = recordFormats;
        this.importConfiguration = importConfiguration;
        this.logProvider = logService.getInternalLogProvider();
        this.toDatabaseLayout = DatabaseLayout.of(toDatabaseDirectory);
        this.fromDatabaseLayout = DatabaseLayout.of(fromDatabaseDirectory);
        this.neo4jConfig = neo4jConfig;
        this.pageCache = pageCache;
        this.ioTracer = ioTracer;
        this.initialIds = initialIds;
        this.externalPageCache = externalPageCache;
        this.idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem );
    }

    private boolean databaseExistsAndContainsData()
    {
        File metaDataFile = toDatabaseLayout.metadataStore();
        try ( PagedFile pagedFile = pageCache.map( metaDataFile, pageCache.pageSize(), StandardOpenOption.READ ) )
        {
            // OK so the db probably exists
        }
        catch ( IOException e )
        {
            // It's OK
            return false;
        }

        try ( NeoStores stores = newStoreFactory(toDatabaseLayout).openNeoStores(StoreType.NODE, StoreType.RELATIONSHIP ) )
        {
            return stores.getNodeStore().getHighId() > 0 || stores.getRelationshipStore().getHighId() > 0;
        }
    }

    /**
     * Called when expecting a clean {@code storeDir} folder and where a new store will be created.
     * This happens on an initial attempt to import.
     *
     * @throws IOException           on I/O error.
     * @throws IllegalStateException if {@code storeDir} already contains a database.
     */
    public void createNew() throws IOException
    {
        assertDatabaseIsEmptyOrNonExistent();

        // There may have been a previous import which was killed before it even started, where the label scan store could
        // be in a semi-initialized state. Better to be on the safe side and deleted it. We get her after determining that
        // the db is either completely empty or non-existent anyway, so deleting this file is OK.
        //fileSystem.deleteFile(getLabelScanStoreFile(databaseLayout));

        instantiateStores();
        /*fromNeoStores.getMetaDataStore().setLastCommittedAndClosedTransactionId(
                initialIds.lastCommittedTransactionId(), initialIds.lastCommittedTransactionChecksum(),
                BASE_TX_COMMIT_TIMESTAMP, initialIds.lastCommittedTransactionLogByteOffset(),
                initialIds.lastCommittedTransactionLogVersion());*/
        toNeoStores.startCountStore();
        fromNeoStores.startCountStore();

        assert toNeoStores != null;
        assert fromNeoStores != null;
    }

    public void assertDatabaseIsEmptyOrNonExistent()
    {
        if ( databaseExistsAndContainsData() )
        {
            throw new IllegalStateException( toDatabaseLayout.databaseDirectory() + " already contains data, cannot do import here" );
        }
    }

    private void deleteStoreFiles( DatabaseLayout databaseLayout, Predicate<StoreType> storesToKeep )
    {
        for ( StoreType type : StoreType.values() )
        {
            if ( type.isRecordStore() && !storesToKeep.test( type ) )
            {
                DatabaseFile databaseFile = type.getDatabaseFile();
                databaseLayout.file( databaseFile ).forEach( fileSystem::deleteFile );
                databaseLayout.idFile( databaseFile ).ifPresent( fileSystem::deleteFile );
            }
        }
    }

    private void instantiateKernelExtensions()
    {
        life = new LifeSupport();
        life.start();
        /*labelScanStore = new NativeLabelScanStore( pageCache, databaseLayout, fileSystem, FullStoreChangeStream.EMPTY, false, new Monitors(),
                RecoveryCleanupWorkCollector.immediate() );
        life.add( labelScanStore );*/
    }

    private boolean instantiateStores() throws IOException
    {
        fromNeoStores = newStoreFactory(fromDatabaseLayout).openAllNeoStores(true);
        FileUtils.copyRecursively(fromDatabaseLayout.databaseDirectory(), toDatabaseLayout.databaseDirectory());
        Predicate<StoreType> predicate = new Predicate<StoreType>()
        {
            private StoreType[] storesToCluster = {StoreType.NODE, StoreType.RELATIONSHIP, StoreType.PROPERTY};
            @Override
            public boolean test( StoreType storeType )
            {
                for ( int i = 0; i < storesToCluster.length; i++ )
                {
                    if ( storeType == storesToCluster[i] )
                    {
                        return false;
                    }
                }
                return true;
            }
        };
        deleteStoreFiles(toDatabaseLayout, predicate);

        toNeoStores = newStoreFactory(toDatabaseLayout).openAllNeoStores(true);

        /*propertyKeyRepository = new BatchingTokenRepository.BatchingPropertyKeyTokenRepository(
                neoStores.getPropertyKeyTokenStore() );
        labelRepository = new BatchingTokenRepository.BatchingLabelTokenRepository(
                neoStores.getLabelTokenStore() );
        relationshipTypeRepository = new BatchingTokenRepository.BatchingRelationshipTypeTokenRepository(
                neoStores.getRelationshipTypeTokenStore() );
        temporaryNeoStores = instantiateTempStores();*/
        instantiateKernelExtensions();

        /*
         Delete the id generators because makeStoreOk isn't atomic in the sense that there's a possibility of an unlucky timing such
         that if the process is killed at the right time some store may end up with a .id file that looks to be CLEAN and has highId=0,
         i.e. effectively making the store look empty on the next start. Normal recovery of a db is sort of protected by this recovery
         recognizing that the db needs recovery when it looks at the tx log and also calling deleteIdGenerators. In the import case
         there are no tx logs at all, and therefore we do this manually right here.
        */
        toNeoStores.deleteIdGenerators();
        //temporaryNeoStores.deleteIdGenerators();

        toNeoStores.makeStoreOk();
        //temporaryNeoStores.makeStoreOk();
        return true;
    }
    public static ClusteringNeoStores ClusteringNeoStores( FileSystemAbstraction fileSystem, File toStoreDir, File fromStoreDir,
                                                      RecordFormats recordFormats, Configuration config, LogService logService, AdditionalInitialIds initialIds,
                                                      Config dbConfig, JobScheduler jobScheduler )
    {
        Config neo4jConfig = getNeo4jConfig( config, dbConfig );
        final PageCacheTracer tracer = new DefaultPageCacheTracer();
        PageCache pageCache = createPageCache( fileSystem, neo4jConfig, logService.getInternalLogProvider(), tracer,
                DefaultPageCursorTracerSupplier.INSTANCE, EmptyVersionContextSupplier.EMPTY, jobScheduler );

        return new ClusteringNeoStores( fileSystem, pageCache, toStoreDir, fromStoreDir, recordFormats, neo4jConfig, config, logService,
                initialIds, false, tracer::bytesWritten );
    }

    public static ClusteringNeoStores ClusteringNeoStoresWithExternalPageCache( FileSystemAbstraction fileSystem,
                                                                            PageCache pageCache, PageCacheTracer tracer, File toStoreDir, File fromStoreDir,
                                                                            RecordFormats recordFormats, Configuration config, LogService logService,
                                                                            AdditionalInitialIds initialIds, Config dbConfig )
    {
        Config neo4jConfig = getNeo4jConfig( config, dbConfig );

        return new ClusteringNeoStores( fileSystem, pageCache, toStoreDir, fromStoreDir, recordFormats, neo4jConfig, config, logService,
                initialIds, true, tracer::bytesWritten );
    }

    private static Config getNeo4jConfig( Configuration config, Config dbConfig )
    {
        dbConfig.augment( stringMap(
                dense_node_threshold.name(), valueOf( config.denseNodeThreshold() ),
                pagecache_memory.name(), valueOf( config.pageCacheMemory() ) ) );
        return dbConfig;
    }

    private static PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, LogProvider log,
                                             PageCacheTracer tracer, PageCursorTracerSupplier cursorTracerSupplier,
                                             VersionContextSupplier contextSupplier, JobScheduler jobScheduler )
    {
        return new ConfiguringPageCacheFactory( fileSystem, config, tracer, cursorTracerSupplier,
                log.getLog( BatchingNeoStores.class ), contextSupplier, jobScheduler ).getOrCreatePageCache();
    }

    private StoreFactory newStoreFactory( DatabaseLayout databaseLayout, OpenOption... openOptions )
    {
        return new StoreFactory( databaseLayout, neo4jConfig, idGeneratorFactory, pageCache, fileSystem, recordFormats, logProvider,
                EmptyVersionContextSupplier.EMPTY, openOptions );
    }

    public IoTracer getIoTracer()
    {
        return ioTracer;
    }

    public NodeStore getToNodeStore()
    {
        return toNeoStores.getNodeStore();
    }
    public NodeStore getFromNodeStore()
    {
        return fromNeoStores.getNodeStore();
    }

    public PropertyStore getToPropertyStore()
    {
        return toNeoStores.getPropertyStore();
    }

    public PropertyStore getFromPropertyStore()
    {
        return fromNeoStores.getPropertyStore();
    }

    public RelationshipStore getToRelationshipStore()
    {
        return toNeoStores.getRelationshipStore();
    }

    public RelationshipStore getFromRelationshipStore()
    {
        return fromNeoStores.getRelationshipStore();
    }

    public RelationshipGroupStore getToRelationshipGroupStore()
    {
        return toNeoStores.getRelationshipGroupStore();
    }

    public RelationshipGroupStore getFromRelationshipGroupStore()
    {
        return fromNeoStores.getRelationshipGroupStore();
    }

    public CountsTracker getToCountsStore()
    {
        return toNeoStores.getCounts();
    }

    public CountsTracker getFromCountsStore()
    {
        return fromNeoStores.getCounts();
    }

    @Override
    public void close() throws IOException
    {
        // Here as a safety mechanism when e.g. panicking.
        if ( flusher != null )
        {
            stopFlushingPageCache();
        }

        flushAndForce();

        // Flush out all pending changes
        //closeAll( propertyKeyRepository, labelRepository, relationshipTypeRepository );

        // Close the neo store
        life.shutdown();
        closeAll( toNeoStores, fromNeoStores );
        if ( !externalPageCache )
        {
            pageCache.close();
        }

        if ( successful )
        {
            cleanup();
        }
    }

    private void cleanup() throws IOException
    {
        /*File tempStoreDirectory = temporaryDatabaseLayout.getStoreLayout().storeDirectory();
        if ( !tempStoreDirectory.getParentFile().equals( databaseLayout.databaseDirectory() ) )
        {
            throw new IllegalStateException( "Temporary store is dislocated. It should be located under current database directory but instead located in: " +
                    tempStoreDirectory.getParent() );
        }
        fileSystem.deleteRecursively( tempStoreDirectory );*/
    }

    public long getLastCommittedTransactionId()
    {
        return fromNeoStores.getMetaDataStore().getLastCommittedTransactionId();
    }

    /*public LabelScanStore getLabelScanStore()
    {
        return labelScanStore;
    }*/

    public NeoStores getToNeoStores()
    {
        return toNeoStores;
    }

    public NeoStores getFromNeoStores()
    {
        return fromNeoStores;
    }

    public void startFlushingPageCache()
    {
        if ( importConfiguration.sequentialBackgroundFlushing() )
        {
            if ( flusher != null )
            {
                throw new IllegalStateException( "Flusher already started" );
            }
            flusher = new PageCacheFlusher( pageCache );
            flusher.start();
        }
    }

    public void stopFlushingPageCache()
    {
        if ( importConfiguration.sequentialBackgroundFlushing() )
        {
            if ( flusher == null )
            {
                throw new IllegalStateException( "Flusher not started" );
            }
            flusher.halt();
            flusher = null;
        }
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        visitor.offHeapUsage( pageCache.maxCachedPages() * pageCache.pageSize() );
    }

    public PageCache getPageCache()
    {
        return pageCache;
    }

    public void flushAndForce()
    {
        /*if ( propertyKeyRepository != null )
        {
            propertyKeyRepository.flush();
        }
        if ( labelRepository != null )
        {
            labelRepository.flush();
        }
        if ( relationshipTypeRepository != null )
        {
            relationshipTypeRepository.flush();
        }*/
        if ( toNeoStores != null )
        {
            toNeoStores.flush( UNLIMITED );
            flushIdFiles( toNeoStores, StoreType.values() );
        }
        /*if ( temporaryNeoStores != null )
        {
            temporaryNeoStores.flush( UNLIMITED );
            flushIdFiles( temporaryNeoStores, TEMP_STORE_TYPES );
        }
        if ( labelScanStore != null )
        {
            labelScanStore.force( UNLIMITED );
        }*/
    }

    public void success()
    {
        successful = true;
    }

    private void flushIdFiles( NeoStores neoStores, StoreType[] storeTypes )
    {
        for ( StoreType type : storeTypes )
        {
            if ( type.isRecordStore() )
            {
                RecordStore<AbstractBaseRecord> recordStore = neoStores.getRecordStore( type );
                Optional<File> idFile = toDatabaseLayout.idFile( type.getDatabaseFile() );
                idFile.ifPresent( f -> idGeneratorFactory.create( f, recordStore.getHighId(), false ) );
            }
        }
    }

}

