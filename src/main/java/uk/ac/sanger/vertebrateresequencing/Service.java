package uk.ac.sanger.vertebrateresequencing;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.AbstractMap;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Path("/service")
public class Service {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @GET
    @Path("/closest/{label}/to/{id}")
    public Response pathToLabel(@PathParam("label") String label,
                                @PathParam("id") Long id,
                                @DefaultValue("both") @QueryParam("direction") String dir,
                                @DefaultValue("20") @QueryParam("depth") Integer depth,
                                @DefaultValue("0") @QueryParam("all") Integer all,
                                @QueryParam("properties") String properties_str,
                                @Context GraphDatabaseService db) throws IOException {
        
        Direction direction;
        if (dir.toLowerCase().equals("incoming")) {
            direction = Direction.INCOMING;
        } else if (dir.toLowerCase().equals("outgoing")) {
            direction = Direction.OUTGOING;
        } else {
            direction = Direction.BOTH;
        }
        
        boolean check_literal_props = false;
        boolean check_regex_props = false;
        List<List<String>> properties_literal = new ArrayList<List<String>>();
        List<Map.Entry<String,Pattern>> properties_regex = new ArrayList<>();
        if (properties_str != null && !properties_str.isEmpty()) {
            String[] properties = properties_str.split("@@@");
            for (String property_def_str: properties) {
                String[] property_def = property_def_str.split("@_@");
                if (property_def[0].equals("literal")) {
                    List<String> kv = Arrays.asList(property_def[1], property_def[2]);
                    properties_literal.add(kv);
                    check_literal_props = true;
                }
                else {
                    Pattern regex = Pattern.compile(property_def[2]);
                    Map.Entry<String,Pattern> kv = new AbstractMap.SimpleEntry<>(property_def[1], regex);
                    properties_regex.add(kv);
                    check_regex_props = true;
                }
            }
        }
        
        LabelEvaluator labelEvaluator = new LabelEvaluator(DynamicLabel.label(label));
        PathExpander pathExpander = PathExpanderBuilder.allTypes(direction).build();
        
        TraversalDescription td = db.traversalDescription()
                .breadthFirst()
                .evaluator(labelEvaluator)
                .evaluator(Evaluators.toDepth(depth))
                .expand(pathExpander)
                .uniqueness(Uniqueness.NODE_GLOBAL);
        
        HashMap<Long, HashMap<String, Object>> results = new HashMap<Long, HashMap<String, Object>>();
        try (Transaction tx = db.beginTx()) {
            Node start = db.getNodeById(id);
            
            traversal: for (org.neo4j.graphdb.Path position : td.traverse(start)) {
                Node found = position.endNode();
                
                if (check_literal_props) {
                    for (List<String> kv: properties_literal) {
                        String key = kv.get(0);
                        if (found.hasProperty(key)) {
                            String prop = found.getProperty(key).toString();
                            if (!prop.equals(kv.get(1))) {
                                continue traversal;
                            }
                        }
                        else {
                            continue traversal;
                        }
                    }
                }
                
                if (check_regex_props) {
                    for (Map.Entry<String,Pattern> kv: properties_regex) {
                        String key = kv.getKey().toString();
                        if (found.hasProperty(key)) {
                            String prop = found.getProperty(key).toString();
                            Matcher m = kv.getValue().matcher(prop);
                            if (!m.matches()) {
                                continue traversal;
                            }
                        }
                        else {
                            continue traversal;
                        }
                    }
                }
                
                HashMap<String, Object> props = new HashMap<String, Object>();
                for (String property : found.getPropertyKeys()) {
                    props.put(property, found.getProperty(property));
                }
                results.put(found.getId(), props);
                
                if (all == 0) {
                    break;
                }
            }
        }
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
}
