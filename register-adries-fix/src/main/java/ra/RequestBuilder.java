package ra;

import javax.json.*;
import java.util.Collections;
import java.util.Map;

/**
 * Used by to build a json dictionary from given parameters
 */
public class RequestBuilder {
    public RequestBuilder() {
    }

    public String buildDatastoreSearch(Map<String, String> params, Map<String, String> filters) {
        JsonBuilderFactory factory = Json.createBuilderFactory(Collections.<String, Object>emptyMap());
        JsonObjectBuilder jsonBuilder = factory.createObjectBuilder();
        for(String key : params.keySet()){
            jsonBuilder.add(key, params.get(key));
        }
        JsonObjectBuilder filtersBuilder = factory.createObjectBuilder();
        for (String filter : filters.keySet()) {
            filtersBuilder.add(filter, filters.get(filter));
        }

        jsonBuilder.add("filters", filtersBuilder.build());

        return jsonBuilder.build().toString();
    }

    public String buildDatastoreRequest(Map<String, String> params, JsonArray records){
        JsonBuilderFactory factory = Json.createBuilderFactory(Collections.<String, Object>emptyMap());
        JsonObjectBuilder jsonBuilder = factory.createObjectBuilder();
        for(String key : params.keySet()){
            jsonBuilder.add(key, params.get(key));
        }

        if(records != null){
            jsonBuilder.add("records", records);
        }
        JsonObject request = jsonBuilder.build();
        return request.toString();
    }

    public String buildJsonHttpRequest(Map<String, String> params){
        JsonBuilderFactory factory = Json.createBuilderFactory(Collections.<String, Object>emptyMap());
        JsonObjectBuilder jsonBuilder = factory.createObjectBuilder();
        for(String key : params.keySet()){
            jsonBuilder.add(key, params.get(key));
        }
        return jsonBuilder.build().toString();
    }


    public String buildDatastoreCreateWithFields(Map<String, String> params, Map<String, String> fields){
        JsonBuilderFactory factory = Json.createBuilderFactory(Collections.<String, Object>emptyMap());
        JsonObjectBuilder builder = factory.createObjectBuilder();
        for(String key : params.keySet()){
            builder.add(key, params.get(key));
        }

        if(fields != null && fields.size() > 0){
            JsonArrayBuilder fieldsBuilder = factory.createArrayBuilder();
            JsonObjectBuilder fieldBuilder;
            for(String id : fields.keySet()){
                fieldBuilder = factory.createObjectBuilder();
                fieldBuilder.add("id", id);
                fieldBuilder.add("type", fields.get(id));
                fieldsBuilder.add(fieldBuilder.build());
            }
            builder.add("fields", fieldsBuilder.build());
        }

        return builder.build().toString();
    }

    /* used when initializing dataset in resource that doesn't exist yet, not in use right now */
    public String buildDatastoreResourceCreate(Map<String, String> params){
        JsonBuilderFactory factory = Json.createBuilderFactory(Collections.<String, Object>emptyMap());
        JsonObjectBuilder childBuilder = factory.createObjectBuilder();
        for(String key : params.keySet()){
            childBuilder.add(key, params.get(key));
        }
        JsonObjectBuilder builder2 = factory.createObjectBuilder();
        builder2.add("resource", childBuilder.build());

        return builder2.build().toString();
    }
}
