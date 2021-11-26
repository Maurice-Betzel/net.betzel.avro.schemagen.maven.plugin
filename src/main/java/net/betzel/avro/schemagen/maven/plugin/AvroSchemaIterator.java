package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

// Iterates over an Avro schema
public class AvroSchemaIterator implements Iterable<Schema>, Iterator<Schema> {

    private final Deque<Schema> nodesToIterate;
    private final Set<Integer> iteratedNodes;
    private Schema currentSchema;

    public AvroSchemaIterator(Schema rootSchema) {
        nodesToIterate = new LinkedList<>();
        iteratedNodes = new HashSet<>();
        nodesToIterate.addFirst(rootSchema);
    }

    // Returns the child nodes of schema
    private static List<Schema> getChildNodes(Schema schema) {
        List<Schema> children = new ArrayList<>();

        switch (schema.getType()) {
            case RECORD:
                for (Schema.Field field : schema.getFields()) {
                    children.add(field.schema());
                }
                break;
            case UNION:
                children.addAll(schema.getTypes());
                break;
            case ARRAY:
                children.add(schema.getElementType());
                break;
            case MAP:
                children.add(schema.getValueType());
                break;
        }
        return children;
    }

    @Override
    public Iterator<Schema> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        while (!nodesToIterate.isEmpty()) {
            currentSchema = nodesToIterate.removeFirst();
            Integer objectNum = System.identityHashCode(currentSchema);
            if (!iteratedNodes.contains(objectNum)) {
                iteratedNodes.add(objectNum);
                nodesToIterate.addAll(getChildNodes(currentSchema));
                return true;
            }
        }
        return false;
    }

    @Override
    public Schema next() {
        return currentSchema;
    }
}
