package de.bwaldvogel.mongo.backend.h2;

import java.io.IOException;

import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class H2Database extends AbstractMongoDatabase<Object> {

    private static final String META_PREFIX = "meta.";
    static final String DATABASES_PREFIX = "databases.";

    private MVStore mvStore;

    public H2Database(String databaseName, MongoBackend backend, MVStore mvStore) throws MongoServerException {
        super(databaseName, backend);
        this.mvStore = mvStore;
        initializeNamespacesAndIndexes();
    }

    @Override
    protected Index<Object> openOrCreateUniqueIndex(String collectionName, String key, boolean ascending) {
        MVMap<Object, Object> mvMap = mvStore.openMap(databaseName + "." + collectionName + "._index_" + key);
        return new H2UniqueIndex(key, ascending, mvMap);
    }

    @Override
    protected MongoCollection<Object> openOrCreateCollection(String collectionName, String idField) {
        String fullCollectionName = databaseName + "." + collectionName;
        MVMap<Object, Document> dataMap = mvStore.openMap(DATABASES_PREFIX + fullCollectionName);
        MVMap<String, Object> metaMap = mvStore.openMap(META_PREFIX + fullCollectionName);
        return new H2Collection(databaseName, collectionName, idField, dataMap, metaMap);
    }

    @Override
    protected long getStorageSize() {
        FileStore fileStore = mvStore.getFileStore();
        if (fileStore != null) {
            try {
                return fileStore.getFile().size();
            } catch (IOException e) {
                throw new RuntimeException("Failed to calculate filestore size", e);
            }
        } else {
            return 0;
        }
    }

    @Override
    protected long getFileSize() {
        return getStorageSize();
    }

    @Override
    public void dropCollection(String collectionName) throws MongoServerException {
        super.dropCollection(collectionName);
        String fullCollectionName = getDatabaseName() + "." + collectionName;
        MVMap<Object, Document> dataMap = mvStore.openMap(DATABASES_PREFIX + fullCollectionName);
        MVMap<String, Object> metaMap = mvStore.openMap(META_PREFIX + fullCollectionName);
        mvStore.removeMap(dataMap);
        mvStore.removeMap(metaMap);
    }

    @Override
    public void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName)
            throws MongoServerException {
        super.moveCollection(oldDatabase, collection, newCollectionName);
        String fullCollectionName = collection.getFullName();
        String newFullName = collection.getDatabaseName() + "." + newCollectionName;
        MVMap<Object, Document> dataMap = mvStore.openMap(DATABASES_PREFIX + fullCollectionName);
        MVMap<String, Object> metaMap = mvStore.openMap(META_PREFIX + fullCollectionName);

        mvStore.renameMap(dataMap, DATABASES_PREFIX + newFullName);
        mvStore.renameMap(metaMap, META_PREFIX + newFullName);
    }

}
