package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class AvroSchemaIterator implements Iterable<Schema>, Iterator<Schema> {

    private final Deque<Schema> nodesToIterate;
    private final Set<Integer> iteratedNodes;
    private Schema currentSchema;

    public AvroSchemaIterator(Schema rootSchema) {
        Objects.requireNonNull(rootSchema, "Missing Schema instance!");
        nodesToIterate = new LinkedList();
        iteratedNodes = new HashSet();
        nodesToIterate.addFirst(rootSchema);
    }

    private static List<Schema> getChildNodes(Schema schema) {
        List<Schema> childNodes = new ArrayList<>();

        switch (schema.getType()) {
            case RECORD:
                for (Schema.Field field : schema.getFields()) {
                    childNodes.add(field.schema());
                }
                break;
            case UNION:
                childNodes.addAll(schema.getTypes());
                break;
            case ARRAY:
                childNodes.add(schema.getElementType());
                break;
            case MAP:
                childNodes.add(schema.getValueType());
                break;
        }
        return childNodes;
    }

    @Override
    public Iterator<Schema> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        while (!nodesToIterate.isEmpty()) {
            currentSchema = nodesToIterate.removeFirst();
            Integer identityHashCode = System.identityHashCode(currentSchema);
            if (!iteratedNodes.contains(identityHashCode)) {
                iteratedNodes.add(identityHashCode);
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