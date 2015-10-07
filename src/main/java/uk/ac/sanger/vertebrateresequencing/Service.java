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
    
    private void addNodeDetailsToResults (Node node, HashMap<Long, HashMap<String, Object>> results, String... label) {
        HashMap<String, Object> props = new HashMap<String, Object>();
        for (String property : node.getPropertyKeys()) {
            props.put(property, node.getProperty(property));
        }
        
        if (label.length == 1) {
            props.put("neo4j_label", label[0]);
        }
        
        results.put(node.getId(), props);
    }
    
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
                
                addNodeDetailsToResults(found, results);
                
                if (all == 0) {
                    break;
                }
            }
        }
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    /*
        MATCH (lane)<-[:sequenced]-(second)<-[:prepared]-(sample)
        WHERE id(lane) = {param}.start_id
        OPTIONAL MATCH (sample)-[:gender]->(gender)
        OPTIONAL MATCH (sample)<-[:member]-(taxon:$taxon_labels)
        OPTIONAL MATCH (sample)<-[:sample]-(donor)
        OPTIONAL MATCH (sample)<-[:member { preferred: 1 }]-(study:$study_labels)
        RETURN lane, second, sample, gender, taxon, study, donor
        
        alternatively
        
        MATCH (lane)<-[:placed]-(sample), (lane)<-[:section]-(second)
        WHERE id(lane) = {param}.start_id
        [and then the same]
    */
    
    enum VrtrackRelationshipTypes implements RelationshipType {
        sequenced, prepared, gender, member, sample, placed, section
    }
    
    // note that this takes a lane or section node id, unlike
    // Schema::VRTrack::get_sequencing_hierarchy which takes a file node id
    @GET
    @Path("/get_sequencing_hierarchy/{database}/{id}") 
    public Response getSequencingHierarchy(@PathParam("database") String database,
                                           @PathParam("id") Long id,
                                           @Context GraphDatabaseService db) throws IOException {
        
        Label taxonLabel = DynamicLabel.label(database + "|VRTrack|Taxon");
        Label studyLabel = DynamicLabel.label(database + "|VRTrack|Study");
        
        HashMap<Long, HashMap<String, Object>> results = new HashMap<Long, HashMap<String, Object>>();
        try (Transaction tx = db.beginTx()) {
            Node lane = db.getNodeById(id);
            if (lane == null) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
            Relationship rel = lane.getSingleRelationship(VrtrackRelationshipTypes.sequenced, Direction.INCOMING);
            
            Node sample = null;
            if (rel != null) {
                Node second = rel.getStartNode();
                if (second != null) {
                    addNodeDetailsToResults(second, results, "Library");
                    rel = second.getSingleRelationship(VrtrackRelationshipTypes.prepared, Direction.INCOMING);
                    if (rel != null) {
                        sample = rel.getStartNode();
                        addNodeDetailsToResults(lane, results, "Lane");
                    }
                }
            }
            else {
                rel = lane.getSingleRelationship(VrtrackRelationshipTypes.section, Direction.INCOMING);
                
                if (rel != null) {
                    Node second = rel.getStartNode();
                    if (second != null) {
                        addNodeDetailsToResults(second, results, "BeadChip");
                        addNodeDetailsToResults(lane, results, "Section");
                    }
                }
                else {
                    return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
                }
                
                rel = lane.getSingleRelationship(VrtrackRelationshipTypes.placed, Direction.INCOMING);
                if (rel != null) {
                    sample = rel.getStartNode();
                }
                else {
                    return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
                }
            }
            
            if (sample == null) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
            addNodeDetailsToResults(sample, results, "Sample");
            
            for (Relationship grel : sample.getRelationships(VrtrackRelationshipTypes.gender, Direction.OUTGOING)) {
                Node gender = grel.getEndNode();
                addNodeDetailsToResults(gender, results, "Gender");
            }
            
            rel = sample.getSingleRelationship(VrtrackRelationshipTypes.sample, Direction.INCOMING);
            if (rel != null) {
                Node donor = rel.getStartNode();
                addNodeDetailsToResults(donor, results, "Donor");
            }
            
            Node preferredStudy = null;
            Node anyStudy = null;
            for (Relationship mrel : sample.getRelationships(VrtrackRelationshipTypes.member, Direction.INCOMING)) {
                Node parent = mrel.getStartNode();
                
                if (parent.hasLabel(taxonLabel)) {
                    addNodeDetailsToResults(parent, results, "Taxon");
                }
                else if (parent.hasLabel(studyLabel)) {
                    // we prefer to only return the first study we find with a
                    // preferred property on mrel, but will settle for any
                    // study if none are preferred
                    if (preferredStudy == null && mrel.hasProperty("preferred")) {
                        String pref = mrel.getProperty("preferred").toString();
                        if (pref.equals("1")) {
                            preferredStudy = parent;
                            addNodeDetailsToResults(preferredStudy, results, "Study");
                        }
                        else {
                            System.out.println(" - the value was NOT 1, it was " + pref);
                        }
                    }
                    else if (anyStudy == null) {
                        anyStudy = parent;
                    }
                }
            }
            
            if (preferredStudy == null && anyStudy != null) {
                addNodeDetailsToResults(anyStudy, results, "Study");
            }
        }

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
}
