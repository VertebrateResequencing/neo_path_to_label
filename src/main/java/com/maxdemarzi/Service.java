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
                                @Context GraphDatabaseService db) throws IOException {
        
        Direction direction;
        if (dir.toLowerCase().equals("incoming")) {
            direction = Direction.INCOMING;
        } else if (dir.toLowerCase().equals("outgoing")) {
            direction = Direction.OUTGOING;
        } else {
            direction = Direction.BOTH;
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
