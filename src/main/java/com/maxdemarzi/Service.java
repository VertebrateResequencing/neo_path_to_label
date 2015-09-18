package com.maxdemarzi;

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
                                @QueryParam("property_key") String property_key,
                                @QueryParam("property_value") String property_value,
                                @QueryParam("property_regex") String property_regex_str,
                                @Context GraphDatabaseService db) throws IOException {
        
        Direction direction;
        if (dir.toLowerCase().equals("incoming")) {
            direction = Direction.INCOMING;
        } else if (dir.toLowerCase().equals("outgoing")) {
            direction = Direction.OUTGOING;
        } else {
            direction = Direction.BOTH;
        }
        
        boolean check_props = false;
        boolean check_regex = false;
        Pattern property_regex = Pattern.compile("local_variable");
        if (property_key != null && !property_key.isEmpty() && ((property_value != null && !property_value.isEmpty()) || (property_regex_str != null && !property_regex_str.isEmpty()))) {
            check_props = true;
            if (property_regex_str != null && !property_regex_str.isEmpty()) {
                check_regex = true;
                property_regex = Pattern.compile(property_regex_str);
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
            
            for (org.neo4j.graphdb.Path position : td.traverse(start)) {
                Node found = position.endNode();
                
                if (check_props) {
                    if (found.hasProperty(property_key)) {
                        String prop = found.getProperty(property_key).toString();
                        if (check_regex) {
                            Matcher m = property_regex.matcher(prop);
                            if (!m.matches()) {
                                continue;
                            }
                        }
                        else {
                            if (!prop.equals(property_value)) {
                                continue;
                            }
                        }
                    }
                    else {
                        continue;
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
