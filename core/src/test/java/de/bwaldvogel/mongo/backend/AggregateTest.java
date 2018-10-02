package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class AggregateTest {

    private static class TestCollection extends AbstractMongoCollection<Object> {

        protected TestCollection(String databaseName, String collectionName, String idField) {
            super(databaseName, collectionName, idField);
        }

        @Override
        protected Object addDocumentInternal(Document document) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int count() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Document getDocument(Object position) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void removeDocument(Object position) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Object findDocumentPosition(Document document) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int getRecordCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int getDeletedCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Iterable<Document> matchDocuments(Document query, Iterable<Object> positions, Document orderBy,
                                                    int numberToSkip, int numberToReturn) throws MongoServerException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip,
                int numberToReturn) throws MongoServerException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void drop() {
        }

        @Override
        protected void updateDataSize(long sizeDelta) {
        }

        @Override
        protected long getDataSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void handleUpdate(Document document) {
            // noop
        }
    }

    private TestCollection collection;

    @Before
    public void setUp() {
        this.collection = new TestCollection("some database", "some collection", "_id");
    }

    @Test
    public void testDeriveDocumentId() throws Exception {
        assertThat(collection.deriveDocumentId(new Document()))
            .isInstanceOf(ObjectId.class);
    }

}