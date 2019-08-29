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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;


import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;

public class RecordCursorHack //extends AutoClosable
{
    public RecordNodeCursor getNodeCursor( NodeStore nodeStore )
    {
        return new RecordNodeCursor( nodeStore );
    }

    public static class RecordRelationshipCursorHack extends RecordRelationshipTraversalCursor
    {
        RecordRelationshipCursorHack( RelationshipStore relationshipStore, RelationshipGroupStore relationshipGroupStore )
        {
            super( relationshipStore, relationshipGroupStore );
        }
    }
    public RecordRelationshipCursorHack getRelationshipCursor( RelationshipStore relationshipStore, RelationshipGroupStore relationshipGroupStore )
    {
        return new RecordRelationshipCursorHack( relationshipStore, relationshipGroupStore );
    }
    public static class RecordPropertyCursorHack extends RecordPropertyCursor
    {
        RecordPropertyCursorHack( PropertyStore propertyStore )
        {
            super( propertyStore );
        }
    }
    public RecordPropertyCursorHack getPropertyCursor( PropertyStore propertyStore )
    {
        return new RecordPropertyCursorHack( propertyStore );
    }
    public static class RecordRelationshipScanCursorHack extends RecordRelationshipScanCursor
    {
        RecordRelationshipScanCursorHack( RelationshipStore relationshipStore )
        {
            super( relationshipStore );
        }
    }
    public RecordRelationshipScanCursorHack getRelationshipScanCursor( RelationshipStore relationshipStore )
    {
        return new RecordRelationshipScanCursorHack( relationshipStore );
    }
    /*
    public static class RecordRelationshipGroupScanCursor
    {
        RelationshipGroupStore relationshipGroupStore;

        RecordRelationshipGroupScanCursor( RelationshipGroupStore relationshipGroupStore )
        {
            this.relationshipGroupStore = relationshipGroupStore;
        }
    }
    */
}
