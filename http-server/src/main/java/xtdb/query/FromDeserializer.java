package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FromDeserializer extends StdDeserializer<Query.From> {
    public FromDeserializer() {
        super(Query.From.class);
    }

    @Override
    public Query.From deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        TypeFactory typeFactory = mapper.getTypeFactory();
        JsonNode node = mapper.readTree(p);

        try {
            var query = Query.from(node.get("from").asText());
            List<OutSpec> outSpecs = new ArrayList<>();

            ArrayNode bindArray= (ArrayNode) node.get("bind");
            for (JsonNode outSpecNode: bindArray) {
                outSpecs.add(mapper.treeToValue(outSpecNode, OutSpec.class));
            }
            return query.binding(outSpecs);
        }
        catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-from"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}
