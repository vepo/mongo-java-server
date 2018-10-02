package de.bwaldvogel.mongo.backend.memory;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.eq;
import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertThat;

import java.util.function.Consumer;

import com.mongodb.client.AggregateIterable;

import org.bson.Document;
import org.junit.Test;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;
import static org.assertj.core.api.Assertions.assertThat;

public class MemoryBackendTest extends AbstractBackendTest {

    @Override
    protected MongoBackend createBackend() {
        return new MemoryBackend();
    }

    @Test
    public void testAggregate() {
        collection.insertOne(json("name: 'OBJECT1', value: 1"));
        collection.insertOne(json("name: 'OBJECT1', value: 2"));
        collection.insertOne(json("name: 'OBJECT1', value: 3"));
        collection.insertOne(json("name: 'OBJECT2', value: 4"));
        collection.insertOne(json("name: 'OBJECT2', value: 5"));
        AggregateIterable<Document> results = collection
                .aggregate(asList(match(eq("name", "OBJECT1")), group(null, sum("value", "$value"))));
        assertThat(results).isNotNull().isNotEmpty().hasSize(1);
        assertThat(results.first().get("value")).isEqualTo(6L);
    }

}