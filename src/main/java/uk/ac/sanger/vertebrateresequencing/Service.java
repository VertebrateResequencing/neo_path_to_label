package uk.ac.sanger.vertebrateresequencing;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.tooling.GlobalGraphOperations;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.AbstractMap;
import java.util.List;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Collections;
import java.util.UUID;

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
    
    enum VrtrackRelationshipTypes implements RelationshipType {
        sequenced, prepared, gender, member, sample, placed, section, has, created_for, aligned,
        administers, failed_by, selected_by, passed_by, passed_genotyping_by, frozen_by, deferred_by, processed, imported,
        converted, discordance,
        cnv_calls, loh_calls, copy_number_by_chromosome_plot, cnv_plot,
        pluritest, pluritest_plot,
        contains, qc_file, genotype_data, summary_stats, verify_bam_id_data, header_mistakes, auto_qc_status,
        moved_from, symlink, copy
    }
    
    private static final Map<RelationshipType, String> fileQCTypes;
    static {
        Map<RelationshipType, String> aMap = new HashMap<RelationshipType, String>();
        aMap.put(VrtrackRelationshipTypes.genotype_data, "Genotype");
        aMap.put(VrtrackRelationshipTypes.summary_stats, "Bam_Stats");
        aMap.put(VrtrackRelationshipTypes.verify_bam_id_data, "Verify_Bam_ID");
        fileQCTypes = Collections.unmodifiableMap(aMap);
    }
    
    Direction in = Direction.INCOMING;
    Direction out = Direction.OUTGOING;
    
    private static Map<String, Node> fseRoots = new HashMap<String, Node>();
    
    private List<org.neo4j.graphdb.Path> getClosestPaths (GraphDatabaseService db, Node start, String label, Direction direction, Integer depth, Integer all, boolean check_literal_props, boolean check_regex_props, List<List<String>> properties_literal, List<Map.Entry<String,Pattern>> properties_regex) {
        LabelEvaluator labelEvaluator = new LabelEvaluator(DynamicLabel.label(label), start.getId());
        PathExpander pathExpander = PathExpanderBuilder.allTypes(direction).build();
        
        TraversalDescription td = db.traversalDescription()
                .breadthFirst()
                .evaluator(labelEvaluator)
                .evaluator(Evaluators.toDepth(depth))
                .expand(pathExpander)
                .uniqueness(Uniqueness.NODE_GLOBAL);
        
        List<org.neo4j.graphdb.Path> paths = new ArrayList<org.neo4j.graphdb.Path>();
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
            
            paths.add(position);
            
            if (all == 0) {
                break;
            }
        }
        return paths;
    }
    
    private List<Node> getClosestNodes (GraphDatabaseService db, Node start, String label, Direction direction, Integer depth, Integer all, boolean check_literal_props, boolean check_regex_props, List<List<String>> properties_literal, List<Map.Entry<String,Pattern>> properties_regex) {
        List<org.neo4j.graphdb.Path> paths = getClosestPaths(db, start, label, direction, depth, all, check_literal_props, check_regex_props, properties_literal, properties_regex);
        List<Node> nodes = new ArrayList<Node>();
        for (org.neo4j.graphdb.Path path: paths) {
            nodes.add(path.endNode());
        }
        return nodes;
    }
    
    @GET
    @Path("/closest/{label}/to/{id}")
    public Response closest(@PathParam("label") String label,
                            @PathParam("id") Long id,
                            @DefaultValue("both") @QueryParam("direction") String dir,
                            @DefaultValue("20") @QueryParam("depth") Integer depth,
                            @DefaultValue("0") @QueryParam("all") Integer all,
                            @QueryParam("properties") String properties_str,
                            @Context GraphDatabaseService db) throws IOException {
        
        Direction direction;
        if (dir.toLowerCase().equals("incoming")) {
            direction = in;
        } else if (dir.toLowerCase().equals("outgoing")) {
            direction = out;
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
        
        HashMap<Long, HashMap<String, Object>> results = new HashMap<Long, HashMap<String, Object>>();
        try (Transaction tx = db.beginTx()) {
            Node start;
            try {
                start = db.getNodeById(id);
            }
            catch (NotFoundException e) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
            List<Node> nodes = getClosestNodes(db, start, label, direction, depth, all, check_literal_props, check_regex_props, properties_literal, properties_regex);
            
            for (Node node : nodes) {
                addNodeDetailsToResults(node, results);
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
        
        (and now it's changed to prefer a study directly attached to lane)
    */
    
    private void getSequencingHierarchy (Node lane, HashMap<Long, HashMap<String, Object>> results, Label taxonLabel, Label studyLabel) {
        Relationship rel = lane.getSingleRelationship(VrtrackRelationshipTypes.sequenced, in);
        
        Node sample = null;
        if (rel != null) {
            Node second = rel.getStartNode();
            if (second != null) {
                addNodeDetailsToResults(second, results, "Library");
                rel = second.getSingleRelationship(VrtrackRelationshipTypes.prepared, in);
                if (rel != null) {
                    sample = rel.getStartNode();
                    addNodeDetailsToResults(lane, results, "Lane");
                }
            }
        }
        else {
            rel = lane.getSingleRelationship(VrtrackRelationshipTypes.section, in);
            
            if (rel != null) {
                Node second = rel.getStartNode();
                if (second != null) {
                    addNodeDetailsToResults(second, results, "BeadChip");
                    addNodeDetailsToResults(lane, results, "Section");
                }
                
                // some sections have 2 (or more?) samples placed in them,
                // resulting in irods gtc files with metadata for 2+ samples...
                // we'll just pick one of the samples at random
                for (Relationship pRel : lane.getRelationships(VrtrackRelationshipTypes.placed, in)) {
                    sample = pRel.getStartNode();
                    break;
                }
                
                if (sample == null) {
                    return;
                }
            }
            else {
                // it's a sequenom|fluidigm csv file directly attached to sample
                rel = lane.getSingleRelationship(VrtrackRelationshipTypes.processed, in);
                
                if (rel != null) {
                    sample = rel.getStartNode();
                }
                else {
                    return;
                }
            }
        }
        
        if (sample == null) {
            return;
        }
        
        addNodeDetailsToResults(sample, results, "Sample");
        
        for (Relationship grel : sample.getRelationships(VrtrackRelationshipTypes.gender, out)) {
            Node gender = grel.getEndNode();
            addNodeDetailsToResults(gender, results, "Gender");
        }
        
        rel = sample.getSingleRelationship(VrtrackRelationshipTypes.sample, in);
        if (rel != null) {
            Node donor = rel.getStartNode();
            addNodeDetailsToResults(donor, results, "Donor");
        }
        
        Node directlyAttachedStudy = null;
        rel = lane.getSingleRelationship(VrtrackRelationshipTypes.created_for, out);
        if (rel != null) {
            directlyAttachedStudy = rel.getEndNode();
            addNodeDetailsToResults(directlyAttachedStudy, results, "Study");
        }
        
        Node preferredStudy = null;
        Node anyStudy = null;
        for (Relationship mrel : sample.getRelationships(VrtrackRelationshipTypes.member, in)) {
            Node parent = mrel.getStartNode();
            
            if (parent.hasLabel(taxonLabel)) {
                addNodeDetailsToResults(parent, results, "Taxon");
            }
            else if (directlyAttachedStudy == null && parent.hasLabel(studyLabel)) {
                // we prefer to only return the first study we find with a
                // preferred property on mrel, but will settle for any
                // study if none are preferred
                if (preferredStudy == null && mrel.hasProperty("preferred")) {
                    String pref = mrel.getProperty("preferred").toString();
                    if (pref.equals("1")) {
                        preferredStudy = parent;
                        addNodeDetailsToResults(preferredStudy, results, "Study");
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
    
    // note that this takes a lane or section node id, unlike
    // Schema::VRTrack::get_sequencing_hierarchy which takes a file node id
    // (with the exception of files that are directly attached to samples)
    @GET
    @Path("/get_sequencing_hierarchy/{database}/{id}") 
    public Response getSequencingHierarchy(@PathParam("database") String database,
                                           @PathParam("id") Long id,
                                           @Context GraphDatabaseService db) throws IOException {
        
        Label taxonLabel = DynamicLabel.label(database + "|VRTrack|Taxon");
        Label studyLabel = DynamicLabel.label(database + "|VRTrack|Study");
        
        HashMap<Long, HashMap<String, Object>> results = new HashMap<Long, HashMap<String, Object>>();
        try (Transaction tx = db.beginTx()) {
            Node lane; // or section or sequenom|fluidgm csv file
            try {
                lane = db.getNodeById(id);
            }
            catch (NotFoundException e) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
            getSequencingHierarchy(lane, results, taxonLabel, studyLabel);
        }

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    /*
        We combine what was previously a bunch of related cypher queries in
        to one efficient set of java api calls:
        
        MATCH (donor)-[:sample]->(sample) WHERE id(donor) = {donor}.id
        MATCH (donor)<-[:member]-(study:$study_labels)<-[:has]-(group:$group_labels)
        OPTIONAL MATCH (group)<-[ar:administers]-(auser:$user_labels)
        OPTIONAL MATCH (sample)-[fbr:failed_by]->(fuser:$user_labels)
        OPTIONAL MATCH (sample)-[sbr:selected_by]->(suser:$user_labels)
        OPTIONAL MATCH (sample)-[pbr:passed_by]->(puser:$user_labels)
        return donor,sample,group,ar,auser,fbr,fuser,sbr,suser,pbr,puser
        
        MATCH (donor)-[:sample]->(sample)-[ser:gender]->(egender) WHERE id(donor) = {donor}.id
        MATCH (sample)-[sar1:processed]->()-[sar2:imported]->()-[sar3:converted]->(resultfile)-[sar4:gender]->(agender)
        return sample, ser, egender, sar1, sar2, sar3, resultfile, sar4, agender
        
        MATCH (donor)-[:sample]->(sample)-[sdr:discordance]->(disc)
        WHERE id(donor) = {donor}.id
        MATCH (study:$study_labels)-[ssr:member]->(sample)
        RETURN study, ssr, sample, sdr, disc
        (but actually we only want the most recently created Discordance node
        attached to each of our samples, per type (fluidigm or genotype))
        
        MATCH (donor)-[:sample]->(sample)-[ssr:cnv_calls]->(summary)
        WHERE id(donor) = {donor}.id WITH sample,ssr,summary
        OPTIONAL MATCH (sample)-[scr:cnv_plot]->(cnvplot)
        OPTIONAL MATCH (sample)-[scnr:copy_number_by_chromosome_plot]->(cnplot)
        OPTIONAL MATCH (sample)-[slr:loh_calls]->(loh)
        RETURN sample,ssr,summary,scr,cnvplot,scnr,cnplot,slr,loh
        
        MATCH (donor)-[:sample]->(sample)-[sdr:pluritest]->(details)
        WHERE id(donor) = {donor}.id WITH donor,sample,sdr,details
        MATCH (donor)-[:pluritest_plot]->(plots)
        RETURN plots,sample,sdr,details
    */
    
    @GET
    @Path("/donor_qc/{database}/{user}/{id}") 
    public Response getDonorQC(@PathParam("database") String database,
                               @PathParam("user") String userName,
                               @PathParam("id") Long donorNodeId,
                               @QueryParam("sample") String sampleToSet,
                               @QueryParam("status") String statusToSet,
                               @QueryParam("reason") String reasonToSet,
                               @QueryParam("time") String timeToSet,
                               @Context GraphDatabaseService db) throws IOException {
        
        Label studyLabel = DynamicLabel.label(database + "|VRTrack|Study");
        Label groupLabel = DynamicLabel.label(database + "|VRTrack|Group");
        Label userLabel = DynamicLabel.label(database + "|VRTrack|User");
        String desiredSampleProps[] = { "name", "public_name", "control" };
        boolean setSampleQC = false;
        if (sampleToSet != null && statusToSet != null) {
            if (statusToSet.equals("failed")) {
                if (reasonToSet != null) {
                    setSampleQC = true;
                }
            }
            else if (statusToSet.equals("set_passed") || statusToSet.equals("set_passed_genotyping") || statusToSet.equals("unset_passed") || statusToSet.equals("unset_passed_genotyping") || statusToSet.equals("selected") || statusToSet.equals("defer") || statusToSet.equals("freeze")) {
                setSampleQC = true;
                reasonToSet = null;
            }
        }
        if (setSampleQC && timeToSet == null) {
            timeToSet = String.valueOf((System.currentTimeMillis() / 1000)); 
        }
        JSONParser jsonParser = new JSONParser();
        
        HashMap<String, HashMap<String, Object>> results = new HashMap<String, HashMap<String, Object>>();
        HashMap<String, Object> donorDetails = new HashMap<String, Object>();
        boolean isAdmin = false;
        HashMap<String, Integer> qcFailHash = new HashMap<String, Integer>();
        HashMap<String, Object> samples = new HashMap<String, Object>();
        try (Transaction tx = db.beginTx()) {
            Node donor;
            try {
                donor = db.getNodeById(donorNodeId);
            }
            catch (NotFoundException e) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
            // we want to know if the supplied user administers any group that
            // has a study that our donor is a member of, and we want to collect
            // all qc_fail_reasons from those groups if so
            Node adminUser = null;
            for (Relationship dsrel: donor.getRelationships(VrtrackRelationshipTypes.member, in)) {
                Node study = dsrel.getStartNode();
                if (study.hasLabel(studyLabel)) {
                    groupLoop: for (Relationship sgrel : study.getRelationships(VrtrackRelationshipTypes.has, in)) {
                        Node group = sgrel.getStartNode();
                        if (group.hasLabel(groupLabel)) {
                            for (Relationship gurel : group.getRelationships(VrtrackRelationshipTypes.administers, in)) {
                                Node user = gurel.getStartNode();
                                if (user.hasLabel(userLabel)) {
                                    String thisUserName = user.getProperty("username").toString();
                                    if (thisUserName.equals(userName)) {
                                        isAdmin = true;
                                        adminUser = user;
                                        
                                        Object qcFailReasons = group.getProperty("qc_fail_reasons", null);
                                        if (qcFailReasons != null) {
                                            if (qcFailReasons instanceof String) {
                                                qcFailHash.put(qcFailReasons.toString(), Integer.valueOf(1));
                                            }
                                            else if (qcFailReasons instanceof String[]) {
                                                int qcFRLength = Array.getLength(qcFailReasons);
                                                for (int i = 0; i < qcFRLength; i ++) {
                                                    String reason = Array.get(qcFailReasons, i).toString();
                                                    qcFailHash.put(reason, Integer.valueOf(1));
                                                }
                                            }
                                        }
                                        
                                        continue groupLoop;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            if (isAdmin && setSampleQC && statusToSet.equals("failed")) {
                if (! qcFailHash.containsKey(reasonToSet)) {
                    setSampleQC = false;
                }
            }
            
            donorDetails.put("id", donor.getProperty("id").toString()); 
            
            // we want the details of all the samples under our donor
            HashMap<String, List<Map.Entry<Node,HashMap<String, Object>>>> studyToSampleInfo = new HashMap<String, List<Map.Entry<Node,HashMap<String, Object>>>>();
            HashMap<String, Integer> donorSampleNames = new HashMap<String, Integer>();
            String cnbcPlot = null;
            for (Relationship dsrel: donor.getRelationships(VrtrackRelationshipTypes.sample, out)) {
                Node sample = dsrel.getEndNode();
                donorSampleNames.put(sample.getProperty("name").toString(), Integer.valueOf(1));
                
                // first set new qc status if supplied
                if (isAdmin && setSampleQC && sample.getProperty("name").equals(sampleToSet)) {
                    RelationshipType suRelType = null;
                    List<RelationshipType> qcRelsToRemove = new ArrayList<RelationshipType>();
                    switch (statusToSet) {
                        case "failed":
                            sample.setProperty("qc_failed", "1");
                            sample.setProperty("qc_selected", "0");
                            sample.setProperty("qc_freeze", "0");
                            sample.setProperty("qc_defer", "0");
                            suRelType = VrtrackRelationshipTypes.failed_by;
                            qcRelsToRemove.add(VrtrackRelationshipTypes.failed_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.selected_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.frozen_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.deferred_by);
                            break;
                        case "selected":
                            sample.setProperty("qc_failed", "0");
                            sample.setProperty("qc_selected", "1");
                            sample.setProperty("qc_freeze", "0");
                            sample.setProperty("qc_defer", "0");
                            suRelType = VrtrackRelationshipTypes.selected_by;
                            qcRelsToRemove.add(VrtrackRelationshipTypes.failed_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.selected_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.frozen_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.deferred_by);
                            break;
                        case "freeze":
                            sample.setProperty("qc_failed", "0");
                            sample.setProperty("qc_selected", "0");
                            sample.setProperty("qc_freeze", "1");
                            sample.setProperty("qc_defer", "0");
                            suRelType = VrtrackRelationshipTypes.frozen_by;
                            qcRelsToRemove.add(VrtrackRelationshipTypes.failed_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.selected_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.frozen_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.deferred_by);
                            break;
                        case "defer":
                            sample.setProperty("qc_failed", "0");
                            sample.setProperty("qc_selected", "0");
                            sample.setProperty("qc_freeze", "0");
                            sample.setProperty("qc_defer", "1");
                            suRelType = VrtrackRelationshipTypes.deferred_by;
                            qcRelsToRemove.add(VrtrackRelationshipTypes.failed_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.selected_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.frozen_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.deferred_by);
                            break;
                        case "set_passed":
                            sample.setProperty("qc_passed", "1");
                            suRelType = VrtrackRelationshipTypes.passed_by;
                            qcRelsToRemove.add(VrtrackRelationshipTypes.passed_by);
                            break;
                        case "unset_passed":
                            sample.setProperty("qc_passed", "0");
                            qcRelsToRemove.add(VrtrackRelationshipTypes.passed_by);
                            qcRelsToRemove.add(VrtrackRelationshipTypes.passed_genotyping_by);
                            break;
                        case "set_passed_genotyping":
                            sample.setProperty("qc_passed_genotyping", "1");
                            suRelType = VrtrackRelationshipTypes.passed_genotyping_by;
                            qcRelsToRemove.add(VrtrackRelationshipTypes.passed_genotyping_by);
                            break;
                        case "unset_passed_genotyping":
                            sample.setProperty("qc_passed_genotyping", "0");
                            qcRelsToRemove.add(VrtrackRelationshipTypes.passed_genotyping_by);
                            break;
                    } 
                    
                    // remove the existing rels
                    for (RelationshipType qcRelType : qcRelsToRemove) {
                        Relationship qcRel = sample.getSingleRelationship(qcRelType, out);
                        if (qcRel != null) {
                            qcRel.delete();
                        }
                    }
                    
                    // now say who did this qc status change, when and why
                    if (suRelType != null) {
                        Relationship qcRel = sample.createRelationshipTo(adminUser, suRelType);
                        qcRel.setProperty("time", timeToSet);
                        if (reasonToSet != null) {
                            qcRel.setProperty("reason", reasonToSet);
                        }
                    }
                }
                
                // get the general sample details we're interested in
                HashMap<String, Object> sampleInfo = new HashMap<String, Object>();
                
                for (String prop: desiredSampleProps) {
                    Object val = sample.getProperty(prop, null);
                    if (val != null) {
                        sampleInfo.put(prop, val);
                    }
                }
                
                // get the sample qc status
                String qcStatus = "pending";
                Relationship qcRel = null;
                boolean getReason = false;
                Object qcVal = sample.getProperty("qc_failed", null);
                if (qcVal != null && qcVal.equals("1")) {
                    qcStatus = "failed";
                    getReason = true;
                    qcRel = sample.getSingleRelationship(VrtrackRelationshipTypes.failed_by, out);
                }
                else {
                    qcVal = sample.getProperty("qc_selected", null);
                    if (qcVal != null && qcVal.equals("1")) {
                        qcStatus = "selected";
                        qcRel = sample.getSingleRelationship(VrtrackRelationshipTypes.selected_by, out);
                    }
                    else {
                        qcVal = sample.getProperty("qc_freeze", null);
                        if (qcVal != null && qcVal.equals("1")) {
                            qcStatus = "freeze";
                            qcRel = sample.getSingleRelationship(VrtrackRelationshipTypes.frozen_by, out);
                        }
                        else {
                            qcVal = sample.getProperty("qc_defer", null);
                            if (qcVal != null && qcVal.equals("1")) {
                                qcStatus = "defer";
                                qcRel = sample.getSingleRelationship(VrtrackRelationshipTypes.deferred_by, out);
                            }
                        }
                    }
                }
                sampleInfo.put("qc_status", qcStatus);
                
                // independently of qc_status we'll return if either of the
                // pass levels have been set
                qcVal = sample.getProperty("qc_passed_genotyping", null);
                if (qcVal != null && qcVal.equals("1")) {
                    sampleInfo.put("qc_passed_fluidigm", true);
                    sampleInfo.put("qc_passed_genotyping", true);
                    if (qcRel == null) {
                        qcRel = sample.getSingleRelationship(VrtrackRelationshipTypes.passed_genotyping_by, out);
                    }
                }
                else {
                    qcVal = sample.getProperty("qc_passed", null);
                    if (qcVal != null && qcVal.equals("1")) {
                        sampleInfo.put("qc_passed_fluidigm", true);
                        sampleInfo.put("qc_passed_genotyping", false);
                        if (qcRel == null) {
                            qcRel = sample.getSingleRelationship(VrtrackRelationshipTypes.passed_by, out);
                        }
                    }
                    else {
                        sampleInfo.put("qc_passed_fluidigm", false);
                        sampleInfo.put("qc_passed_genotyping", false);
                    }
                }
                
                if (qcRel != null) {
                    Node qcUser = qcRel.getEndNode();
                    sampleInfo.put("qc_by", qcUser.getProperty("username", "unknown"));
                    sampleInfo.put("qc_time", qcRel.getProperty("time", "0"));
                    if (getReason) {
                        sampleInfo.put("qc_failed_reason", qcRel.getProperty("reason", "0"));
                    }
                }
                
                // get expected gender
                for (Relationship sgrel : sample.getRelationships(VrtrackRelationshipTypes.gender, out)) {
                    Node gender = sgrel.getEndNode();
                    if (gender.getProperty("source").equals("sequencescape")) {
                        sampleInfo.put("expected_gender", gender.getProperty("gender"));
                        break;
                    }
                }
                
                // get calculated actual gender
                calcGenderLoop: for (Relationship sar1 : sample.getRelationships(VrtrackRelationshipTypes.processed, out)) {
                    Node proc = sar1.getEndNode();
                    for (Relationship sar2 : proc.getRelationships(VrtrackRelationshipTypes.imported, out)) {
                        Node importNode = sar2.getEndNode();
                        for (Relationship sar3 : importNode.getRelationships(VrtrackRelationshipTypes.converted, out)) {
                            Node resultfile = sar3.getEndNode();
                            for (Relationship sar4 : resultfile.getRelationships(VrtrackRelationshipTypes.gender, out)) {
                                Node gender = sar4.getEndNode();
                                sampleInfo.put("actual_gender", gender.getProperty("gender"));
                                sampleInfo.put("actual_gender_result_file", resultfile.getProperty("path"));
                                break calcGenderLoop;
                            }
                        }
                    }
                }
                
                // later we'll get the most recent fluidigm discordance results
                // for this sample. We're only interested in the results between
                // sample pairs where at least one sample is in the largest
                // study, so here we only generate study counts
                ArrayList<Integer> sampleStudies = new ArrayList<Integer>();
                for (Relationship ssRel : sample.getRelationships(VrtrackRelationshipTypes.member, in)) {
                    Node study = ssRel.getStartNode();
                    if (! study.hasLabel(studyLabel)) {
                        continue;
                    }
                    
                    String studyID = study.getProperty("id").toString();
                    sampleStudies.add(Integer.parseInt(studyID));
                    
                    List<Map.Entry<Node,HashMap<String, Object>>> samplesList = null;
                    if (studyToSampleInfo.containsKey(studyID)) {
                        samplesList = studyToSampleInfo.get(studyID);
                    }
                    else {
                        samplesList = new ArrayList<>();
                        studyToSampleInfo.put(studyID, samplesList);
                    }
                    java.util.Map.Entry<Node,HashMap<String, Object>> nodeAndInfo = new java.util.AbstractMap.SimpleEntry<>(sample, sampleInfo);
                    samplesList.add(nodeAndInfo);
                }
                
                Collections.sort(sampleStudies);
                sampleInfo.put("study_ids", StringUtils.join(sampleStudies, ","));
                
                // get the latest cnv calls
                Node callNode = null;
                int maxEpoch = 0;
                for (Relationship scnvRel : sample.getRelationships(VrtrackRelationshipTypes.cnv_calls, out)) {
                    Node thisNode = scnvRel.getEndNode();
                    int thisEpoch = Integer.parseInt(thisNode.getProperty("date").toString());
                    if (thisEpoch > maxEpoch) {
                        callNode = thisNode;
                        maxEpoch = thisEpoch;
                    }
                }
                if (callNode != null) {
                    sampleInfo.put("cnv_calls", callNode.getProperty("data").toString());
                }
                
                // get the latest loh calls
                callNode = null;
                maxEpoch = 0;
                for (Relationship slohRel : sample.getRelationships(VrtrackRelationshipTypes.loh_calls, out)) {
                    Node thisNode = slohRel.getEndNode();
                    int thisEpoch = Integer.parseInt(thisNode.getProperty("date").toString());
                    if (thisEpoch > maxEpoch) {
                        callNode = thisNode;
                        maxEpoch = thisEpoch;
                    }
                }
                if (callNode != null) {
                    sampleInfo.put("loh_calls", callNode.getProperty("data").toString());
                }
                
                // get the single copy_number_by_chromosome_plot shared by
                // all samples of our donor (but not all samples may have the
                // plot attached, so we keep testing till we find it)
                if (cnbcPlot == null) {
                    Relationship scnbcpRel = sample.getSingleRelationship(VrtrackRelationshipTypes.copy_number_by_chromosome_plot, out);
                    if (scnbcpRel != null) {
                        Node cnbcpNode = scnbcpRel.getEndNode();
                        cnbcPlot = cnbcpNode.getProperty("path").toString();
                        donorDetails.put("copy_number_by_chromosome_plot", cnbcPlot);
                    }
                }
                
                // get all the cnv plots (there could be 1 per chromosome)
                for (Relationship scpRel : sample.getRelationships(VrtrackRelationshipTypes.cnv_plot, out)) {
                    Node plot = scpRel.getEndNode();
                    String key = "cnv_plot_" + plot.getProperty("chr");
                    sampleInfo.put(key, plot.getProperty("path").toString());
                }
                
                // get the latest pluritest calls
                callNode = null;
                maxEpoch = 0;
                for (Relationship spRel : sample.getRelationships(VrtrackRelationshipTypes.pluritest, out)) {
                    Node thisNode = spRel.getEndNode();
                    int thisEpoch = Integer.parseInt(thisNode.getProperty("date").toString());
                    if (thisEpoch > maxEpoch) {
                        callNode = thisNode;
                        maxEpoch = thisEpoch;
                    }
                }
                if (callNode != null) {
                    sampleInfo.put("pluritest_summary", callNode.getProperty("data").toString());
                }
                
                samples.put(String.valueOf(sample.getId()), sampleInfo);
            }
            
            // work out the largest study
            String largestStudy = null;
            int largestStudyCount = 0;
            for (Map.Entry<String, List<Map.Entry<Node,HashMap<String, Object>>>> entry : studyToSampleInfo.entrySet()) {
                int size = entry.getValue().size();
                if (size > largestStudyCount) {
                    largestStudy = entry.getKey();
                    largestStudyCount = size;
                }
            }
            
            // now we have everything we need to get discordance results
            HashMap<String, Integer> doneSamples = new HashMap<String, Integer>();
            for (Map.Entry<String, List<Map.Entry<Node,HashMap<String, Object>>>> entry : studyToSampleInfo.entrySet()) {
                String thisStudy = entry.getKey();
                for (Map.Entry<Node,HashMap<String, Object>> nodeAndInfo : entry.getValue()) {
                    HashMap<String, Object> sampleInfo = nodeAndInfo.getValue();
                    
                    // we don't waste time getting results for failed samples
                    if (sampleInfo.get("qc_status").equals("failed")) {
                        continue;
                    }
                    
                    // a sample can be in multiple studies, but we will try not
                    // to process them more than once (but we have to process
                    // each sample through the largest study)
                    String sampleName = sampleInfo.get("name").toString();
                    if (doneSamples.containsKey(sampleName)) {
                        continue;
                    }
                    
                    // we'll get the latest genotype node for all samples,
                    // and the latest fluidigm node for samples in the
                    // largest study
                    Node sample = nodeAndInfo.getKey();
                    boolean getFluidigm = false;
                    if (thisStudy.equals(largestStudy)) {
                        getFluidigm = true;
                        doneSamples.put(sampleName, Integer.valueOf(1));
                    }
                    
                    HashMap<String, Node> discs = new HashMap<String, Node>();
                    int genotypeDiscEpoch = 0;
                    int fluidigmDiscEpoch = 0;
                    for (Relationship sdRel : sample.getRelationships(VrtrackRelationshipTypes.discordance, out)) {
                        Node disc = sdRel.getEndNode();
                        if (disc.getProperty("type").equals("genotype")) {
                            int thisEpoch = Integer.parseInt(disc.getProperty("date").toString());
                            if (thisEpoch > genotypeDiscEpoch) {
                                discs.put("discordance_genotyping", disc);
                                genotypeDiscEpoch = thisEpoch;
                            }
                        }
                        else if (getFluidigm) {
                            int thisEpoch = Integer.parseInt(disc.getProperty("date").toString());
                            if (thisEpoch > fluidigmDiscEpoch) {
                                discs.put("discordance_fluidigm", disc);
                                fluidigmDiscEpoch = thisEpoch;
                            }
                        }
                    }
                    
                    // disc cns property is a json string and we want to store
                    // a json string of the results vs other samples belonging
                    // to the donor
                    for (Map.Entry<String, Node> discEntry : discs.entrySet()) {
                        String key = discEntry.getKey();
                        Node disc = discEntry.getValue();
                        
                        String cns = disc.getProperty("cns").toString();
                        try {
                            Object cnsObj = jsonParser.parse(cns);
                            JSONObject cnsHash = (JSONObject)cnsObj;
                            JSONArray cnsResults = new JSONArray();
                            for (Object cnsResult : cnsHash.values()) {
                                // fluidigm nodes have results against ALL other
                                // samples, so we need to filter to just those
                                // of our donor; cnsResult is an array with [3]
                                // being the other sample name
                                JSONArray cnsResultJSON = (JSONArray)cnsResult;
                                if (donorSampleNames.containsKey(cnsResultJSON.get(3))) {
                                    cnsResults.add(cnsResult);
                                }
                            }
                            
                            sampleInfo.put(key, cnsResults.toString());
                        }
                        catch (ParseException e) {
                            return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
                        }
                    }
                }
            }
            
            // get pluritest plots which are attached to the donor node
            for (Relationship dppRel: donor.getRelationships(VrtrackRelationshipTypes.pluritest_plot, out)) {
                Node plot = dppRel.getEndNode();
                String key = "pluritest_plot_" + plot.getProperty("type");
                donorDetails.put(key, plot.getProperty("path").toString());
            }
            
            if (setSampleQC) {
                tx.success();
            }
        }
        
        HashMap<String, Object> adminDetails = new HashMap<String, Object>();
        if (isAdmin) {
            adminDetails.put("is_admin", "1");
            
            ArrayList<String> qcFailArray = new ArrayList<String>();
            for (String reason: qcFailHash.keySet()) {
                qcFailArray.add(reason);
            }
            if (qcFailArray.size() > 0) {
                adminDetails.put("qc_fail_reasons", qcFailArray);
            }
        }
        else {
            adminDetails.put("is_admin", "0");
        }
        results.put("admin_details", adminDetails);
        
        if (! samples.isEmpty()) {
            results.put("samples", samples);
        }
        
        results.put("donor_details", donorDetails);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    /*
        MATCH (donor) WHERE id(donor) = {param}.donor_id 
        MATCH (donor)-[ds_rel:sample]->(d_sample)
        RETURN donor,ds_rel,d_sample
        
        MATCH (sample) WHERE id(sample) = {param}.sample_id 
        OPTIONAL MATCH (sample)<-[sd_rel:sample]-(s_donor)
        OPTIONAL MATCH (s_study:$study_labels)-[ssr:member]->(sample)
        RETURN sample,sd_rel,s_donor,ssr,s_study
    */
    
    private void addExtraVRTrackInfo (Node node, HashMap<String, Object> props, Label donorLabel, Label sampleLabel, Label studyLabel) {
        if (node.hasLabel(donorLabel)) {
            props.put("neo4j_label", "Donor");
            
            // add the most recent sample created date, and the name of
            // a control sample, or just the shortest sample name if not
            // control exists. Also count up the number of samples that have
            // unresolved qc at fluidigm and genotyping stages
            int mostRecentDate = 0;
            List<String> controls = new ArrayList<String>();
            String shortest = null;
            int numSamples = 0;
            int numUnresolvedFluidigm = 0; // there is fluidigm data but no qc_passed set
            int numUnresolvedGenotyping = 0; // there is genotyping & microarray data but no final qc status set (qc_passed_genotyping is redundant)
            int numUnresolved = 0; // there are samples but no final qc status set (in case a sample is not expected to ever get genotyping results, or even fluidigm?!)
            for (Relationship dsrel: node.getRelationships(VrtrackRelationshipTypes.sample, out)) {
                Node sample = dsrel.getEndNode();
                
                if (sample.hasProperty("created_date")) {
                    int thisDate = Integer.parseInt(sample.getProperty("created_date").toString());
                    if (thisDate > mostRecentDate) {
                        mostRecentDate = thisDate;
                    }
                }
                
                String name = null;
                if (sample.hasProperty("public_name")) {
                    name = sample.getProperty("public_name").toString();
                }
                else if (sample.hasProperty("name")) {
                    name = sample.getProperty("name").toString();
                }
                if (name != null) {
                    if (shortest == null || name.length() < shortest.length()) {
                        shortest = name;
                    }
                    
                    if (sample.hasProperty("control")) {
                        if (sample.getProperty("control").equals("1")) {
                            controls.add(name);
                        }
                    }
                }
                
                numSamples++;
                
                boolean unresolved = false;
                boolean failed = false;
                Object qcVal = sample.getProperty("qc_failed", null);
                if (qcVal != null && qcVal.equals("1")) {
                    unresolved = false;
                    failed = true;
                }
                else {
                    qcVal = sample.getProperty("qc_selected", null);
                    if (qcVal != null && qcVal.equals("1")) {
                        unresolved = false;
                    }
                    else {
                        qcVal = sample.getProperty("qc_freeze", null);
                        if (qcVal != null && qcVal.equals("1")) {
                            unresolved = false;
                        }
                        else {
                            unresolved = true;
                        }
                    }
                }
                
                boolean hasFluidigm = false;
                boolean hasGenotyping = false;
                for (Relationship sdrel: sample.getRelationships(VrtrackRelationshipTypes.discordance, out)) {
                    Node disc = sdrel.getEndNode();
                    String type = disc.getProperty("type").toString();
                    if (type.equals("fluidigm")) {
                        hasFluidigm = true;
                    }
                    else if (type.equals("genotype")) {
                        hasGenotyping = true;
                        if (hasFluidigm) {
                            break;
                        }
                    }
                }
                
                boolean hasMicroarray = false;
                if (hasGenotyping && sample.hasRelationship(VrtrackRelationshipTypes.pluritest, out)) {
                    hasMicroarray = true;
                }
                
                if (hasFluidigm && ! failed) {
                    qcVal = sample.getProperty("qc_passed", null);
                    if (qcVal == null || qcVal.equals("0")) {
                        numUnresolvedFluidigm++;
                    }
                }
                
                if (unresolved) {
                    numUnresolved++;
                    
                    if (hasMicroarray) {
                        numUnresolvedGenotyping++;
                    }
                }
            }
            
            if (controls.size() == 1) {
                props.put("example_sample", controls.get(0));
            }
            else if (shortest != null) {
                props.put("example_sample", shortest);
            }
            
            if (mostRecentDate > 0) {
                props.put("last_sample_added_date", String.valueOf(mostRecentDate));
            }
            
            props.put("qc_unresolved_fluidigm", String.valueOf(numUnresolvedFluidigm));
            props.put("qc_unresolved_genotyping", String.valueOf(numUnresolvedGenotyping));
            props.put("qc_unresolved", String.valueOf(numUnresolved));
        }
        else if (node.hasLabel(sampleLabel)) {
            props.put("neo4j_label", "Sample");
            
            // add donor ids
            Relationship sdRel = node.getSingleRelationship(VrtrackRelationshipTypes.sample, in);
            if (sdRel != null) {
                Node donor = sdRel.getStartNode();
                props.put("donor_node_id", String.valueOf(donor.getId()));
                props.put("donor_id", donor.getProperty("id").toString());
            }
            
            // add the first (lowest node id) study id, preferred if possible
            Long lowest = null;
            Long lowestPreferred = null;
            String studyId = null;
            String studyIdPreferred = null;
            for (Relationship ssRel: node.getRelationships(VrtrackRelationshipTypes.member, in)) {
                Node study = ssRel.getStartNode();
                if (study.hasLabel(studyLabel)) {
                    Long studyNodeID = study.getId();
                    if (ssRel.hasProperty("preferred")) {
                        if (lowestPreferred == null || studyNodeID < lowestPreferred) {
                            lowestPreferred = studyNodeID;
                            studyIdPreferred = study.getProperty("id").toString();
                        }
                    }
                    else {
                        if (lowest == null || studyNodeID < lowest) {
                            lowest = studyNodeID;
                            studyId = study.getProperty("id").toString();
                        }
                    }
                }
            }
            
            if (studyIdPreferred != null) {
                props.put("study_node_id", String.valueOf(lowestPreferred));
                props.put("study_id", studyIdPreferred);
            }
            else if (studyId != null) {
                props.put("study_node_id", String.valueOf(lowest));
                props.put("study_id", studyId);
            }
        }
    }
    
    @GET
    @Path("/get_node_with_extra_info/{database}/{id}") 
    public Response getNodeWithExtra(@PathParam("database") String database,
                                           @PathParam("id") Long nodeId,
                                           @Context GraphDatabaseService db) throws IOException {
        
        Label donorLabel = DynamicLabel.label(database + "|VRTrack|Donor");
        Label sampleLabel = DynamicLabel.label(database + "|VRTrack|Sample");
        Label studyLabel = DynamicLabel.label(database + "|VRTrack|Study");
        
        HashMap<Long, HashMap<String, Object>> results = new HashMap<Long, HashMap<String, Object>>();
        try (Transaction tx = db.beginTx()) {
            Node node;
            try {
                node = db.getNodeById(nodeId);
            }
            catch (NotFoundException e) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
            addNodeDetailsToResults(node, results);
            addExtraVRTrackInfo(node, results.get(node.getId()), donorLabel, sampleLabel, studyLabel);
        }

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    /*
        MATCH (group:$group_labels) WHERE id(group) IN {group}.ids
        MATCH (group)-->(study:$study_labels) WHERE id(study) IN {study}.ids
        MATCH (study)-->(donor:$donor_labels) WHERE id(donor) IN {donor}.ids
        MATCH (donor)-->(sample:$sample_labels) WHERE id(sample) IN {sample}.ids
        [...]
    */
    
    @GET
    @Path("/vrtrack_nodes/{database}/{label}") 
    public Response getVrtrackNodes(@PathParam("database") String database,
                                           @PathParam("label") String label,
                                           @QueryParam("groups") String groupsStr,
                                           @QueryParam("studies") String studiesStr,
                                           @QueryParam("donors") String donorsStr,
                                           @QueryParam("samples") String samplesStr,
                                           @Context GraphDatabaseService db) throws IOException {
        
        GlobalGraphOperations ggo = GlobalGraphOperations.at(db);
        
        Label desiredLabel = DynamicLabel.label(database + "|VRTrack|" + label);
        Label groupLabel = DynamicLabel.label(database + "|VRTrack|Group");
        Label studyLabel = DynamicLabel.label(database + "|VRTrack|Study");
        Label donorLabel = DynamicLabel.label(database + "|VRTrack|Donor");
        Label sampleLabel = DynamicLabel.label(database + "|VRTrack|Sample");
        
        boolean addExtra = false;
        if (label.equals("Donor") || label.equals("Sample")) {
            addExtra = true;
        }
        
        HashMap<Long, HashMap<String, Object>> results = new HashMap<Long, HashMap<String, Object>>();
        try (Transaction tx = db.beginTx()) {
            if (groupsStr != null) {
                String[] groups = groupsStr.split(",");
                
                for (String groupIdStr: groups) {
                    try {
                        Node group = db.getNodeById(Long.parseLong(groupIdStr));
                        
                        if (group.hasLabel(groupLabel)) {
                            if (label.equals("Study")) {
                                for (Relationship gsRel: group.getRelationships(VrtrackRelationshipTypes.has, out)) {
                                    Node study = gsRel.getEndNode();
                                    
                                    if (study.hasLabel(studyLabel)) {
                                        addNodeDetailsToResults(study, results, label);
                                    }
                                }
                            }
                            else if (studiesStr != null) {
                                String[] studies = studiesStr.split(",");
                                
                                for (String studyIdStr: studies) {
                                    Node study = db.getNodeById(Long.parseLong(studyIdStr));
                                    
                                    if (study.hasLabel(studyLabel)) {
                                        boolean studyInGroup = false;
                                        for (Relationship sgRel: study.getRelationships(VrtrackRelationshipTypes.has, in)) {
                                            Node thisGroup = sgRel.getStartNode();
                                            if (thisGroup.getId() == group.getId()) {
                                                studyInGroup = true;
                                                break;
                                            }
                                        }
                                        if (! studyInGroup) {
                                            continue;
                                        }
                                        
                                        if (donorsStr != null) {
                                            String[] donors = donorsStr.split(",");
                                            
                                            for (String donorIdStr: donors) {
                                                Node donor = db.getNodeById(Long.parseLong(donorIdStr));
                                                
                                                if (donor.hasLabel(donorLabel)) {
                                                    if (samplesStr != null) {
                                                        String[] samples = samplesStr.split(",");
                                                        
                                                        for (String sampleIdStr: samples) {
                                                            Node sample = db.getNodeById(Long.parseLong(sampleIdStr));
                                                            
                                                            Relationship sdRel = sample.getSingleRelationship(VrtrackRelationshipTypes.sample, in);
                                                            if (sdRel != null) {
                                                                Node thisDonor = sdRel.getStartNode();
                                                                if (thisDonor.getId() == donor.getId()) {
                                                                    if (label.equals("Sample")) {
                                                                        addNodeDetailsToResults(sample, results, label);
                                                                        addExtraVRTrackInfo(sample, results.get(sample.getId()), donorLabel, sampleLabel, studyLabel);
                                                                    }
                                                                    else if (label.equals("Donor")) {
                                                                        addNodeDetailsToResults(thisDonor, results, label);
                                                                        addExtraVRTrackInfo(thisDonor, results.get(thisDonor.getId()), donorLabel, sampleLabel, studyLabel);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    else if (addExtra) {
                                                        if (label.equals("Donor")) {
                                                            addNodeDetailsToResults(donor, results, label);
                                                            addExtraVRTrackInfo(donor, results.get(donor.getId()), donorLabel, sampleLabel, studyLabel);
                                                        }
                                                        else if (label.equals("Sample")) {
                                                            for (Relationship dsRel: donor.getRelationships(VrtrackRelationshipTypes.sample, out)) {
                                                                Node sample = dsRel.getEndNode();
                                                                addNodeDetailsToResults(sample, results, label);
                                                                addExtraVRTrackInfo(sample, results.get(sample.getId()), donorLabel, sampleLabel, studyLabel);
                                                            }
                                                        }
                                                        
                                                    }
                                                }
                                            }
                                        }
                                        else if (samplesStr != null) {
                                            String[] samples = samplesStr.split(",");
                                            
                                            for (String sampleIdStr: samples) {
                                                Node sample = db.getNodeById(Long.parseLong(sampleIdStr));
                                                
                                                if (sample.hasLabel(sampleLabel)) {
                                                    if (label.equals("Sample")) {
                                                        addNodeDetailsToResults(sample, results, label);
                                                        addExtraVRTrackInfo(sample, results.get(sample.getId()), donorLabel, sampleLabel, studyLabel);
                                                    }
                                                    else if (label.equals("Donor")) {
                                                        Relationship sdRel = sample.getSingleRelationship(VrtrackRelationshipTypes.sample, in);
                                                        if (sdRel != null) {
                                                            Node donor = sdRel.getStartNode();
                                                            addNodeDetailsToResults(donor, results, label);
                                                            addExtraVRTrackInfo(donor, results.get(donor.getId()), donorLabel, sampleLabel, studyLabel);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        else if (addExtra) {
                                            for (Relationship soRel: study.getRelationships(VrtrackRelationshipTypes.member, out)) {
                                                Node sampleOrDonor = soRel.getEndNode();
                                                
                                                if (sampleOrDonor.hasLabel(desiredLabel)) {
                                                    addNodeDetailsToResults(sampleOrDonor, results, label);
                                                    addExtraVRTrackInfo(sampleOrDonor, results.get(sampleOrDonor.getId()), donorLabel, sampleLabel, studyLabel);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch (NotFoundException e) {
                        continue;
                    }
                }
            }
            else {
                ResourceIterable<Node> allNodes = ggo.getAllNodesWithLabel(desiredLabel);
                for (Node node : allNodes) {
                    addNodeDetailsToResults(node, results, label);
                    if (addExtra) {
                        addExtraVRTrackInfo(node, results.get(node.getId()), donorLabel, sampleLabel, studyLabel);
                    }
                }
            }
        }

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    /*
        We combine what was previously a bunch of related cypher queries in
        to one efficient set of java api calls.
        
        Find the file node by protocol and path (note that protocol should be
        supplied pre-encrypted):
        MATCH (root:$fse_label { basename: { param }.root_basename })"
        -[:contains]->(`$dir_num` { basename: { param }.`${dir_num}_basename` })
        -[:contains]->(leaf:$fse_label { basename: { leaf_basename } })
        USING INDEX leaf:$fse_label(basename) RETURN leaf
        
        Get the file qc nodes:
        MATCH (file) WHERE id(file) = {file}.id
        OPTIONAL MATCH (file)-[:qc_file]->()-[:genotype_data]->(g)
        OPTIONAL MATCH (file)-[:qc_file]->()-[:summary_stats]->(s)
        OPTIONAL MATCH (file)-[:qc_file]->()-[:verify_bam_id_data]->(v)
        OPTIONAL MATCH (file)-[:header_mistakes]->(h)
        RETURN g,s,v,h
        
        We'll also get the closest Lane or Section node and then do what the
        get_sequencing_hierarchy service does.
    */
    
    private Node pathToFSE (GraphDatabaseService db, String dbLabel, Label fseLabel, String root, String path, boolean create) {
        Node rootNode = fseRoots.get(root);
        String rootCreateQuery = "MERGE (n:`" + dbLabel + "`:`" + fseLabel.name() + "` { uuid: {uuid}, basename: {basename} }) RETURN n";
        boolean checkRootNode = false;
        if (rootNode == null) {
            if (! fseRoots.containsKey(root)) {
                rootNode = db.findNode(fseLabel, "basename", root);
                
                if (rootNode == null && create) {
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put("uuid", String.valueOf(UUID.randomUUID()));
                    parameters.put("basename", root);
                    ResourceIterator<Node> resultIterator = db.execute(rootCreateQuery, parameters).columnAs( "n" );
                    rootNode = resultIterator.next();
                }
                
                if (rootNode != null) {
                    fseRoots.put(root, rootNode);
                }
            }
        }
        else {
            checkRootNode = true;
        }
        
        if (path.equals("/")) {
            if (checkRootNode) {
                // possibly only for testing, we need to check cached root nodes
                // are valid
                try {
                    rootNode.hasRelationship(VrtrackRelationshipTypes.contains, out);
                }
                catch (org.neo4j.graphdb.DatabaseShutdownException|org.neo4j.graphdb.NotFoundException err) {
                    rootNode = db.findNode(fseLabel, "basename", root);
                    
                    if (rootNode == null && create) {
                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put("uuid", String.valueOf(UUID.randomUUID()));
                        parameters.put("basename", root);
                        ResourceIterator<Node> resultIterator = db.execute(rootCreateQuery, parameters).columnAs( "n" );
                        rootNode = resultIterator.next();
                    }
                    
                    if (rootNode != null) {
                        fseRoots.put(root, rootNode);
                    }
                }
            }
            
            return rootNode;
        }
        
        String[] basenames = path.split("/");
        basenames = Arrays.copyOfRange(basenames, 1, basenames.length);
        
        if (rootNode != null) {
            String newFSEQuery = "MATCH (s) WHERE id(s) = {pid} MERGE (s)-[:contains]->(n:`" + dbLabel + "`:`" + fseLabel.name() + "` { uuid: {uuid}, path: {path}, basename: {basename} }) RETURN n";
            
            Node leafNode = rootNode;
            boolean creating = false;
            String pathSoFar = "";
            for (String basename: basenames) {
                pathSoFar += "/" + basename;
                Node thisLeaf = null;
                
                if (! creating) {
                    try {
                        for (Relationship dfRel: leafNode.getRelationships(VrtrackRelationshipTypes.contains, out)) {
                            Node fse = dfRel.getEndNode();
                            if (fse.getProperty("basename").toString().equals(basename)) {
                                thisLeaf = fse;
                                break;
                            }
                        }
                    }
                    catch (org.neo4j.graphdb.DatabaseShutdownException|org.neo4j.graphdb.NotFoundException err) {
                        // possibly only happens in testing, but if we store
                        // root nodes and then the db gets "shutdown" and we
                        // reconnect, the stored root nodes will no longer work,
                        // so we fix that now
                        if (leafNode.getId() == rootNode.getId()) {
                            rootNode = db.findNode(fseLabel, "basename", root);
                            
                            if (rootNode == null && create) {
                                Map<String, Object> parameters = new HashMap<>();
                                parameters.put("uuid", String.valueOf(UUID.randomUUID()));
                                parameters.put("basename", root);
                                ResourceIterator<Node> resultIterator = db.execute(rootCreateQuery, parameters).columnAs( "n" );
                                rootNode = resultIterator.next();
                            }
                            
                            if (rootNode != null) {
                                fseRoots.put(root, rootNode);
                                leafNode = rootNode;
                                
                                for (Relationship dfRel: leafNode.getRelationships(VrtrackRelationshipTypes.contains, out)) {
                                    Node fse = dfRel.getEndNode();
                                    if (fse.getProperty("basename").toString().equals(basename)) {
                                        thisLeaf = fse;
                                        break;
                                    }
                                }
                            }
                            else {
                                leafNode = null;
                                break;
                            }
                        }
                    }
                }
                
                if (thisLeaf != null) {
                    leafNode = thisLeaf;
                }
                else {
                    if (create) {
                        // we have to use cypher MERGE to ensure we stick to
                        // our uniqueness constraints
                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put("pid", leafNode.getId());
                        parameters.put("uuid", String.valueOf(UUID.randomUUID()));
                        parameters.put("path", pathSoFar);
                        parameters.put("basename", basename);
                        
                        ResourceIterator<Node> resultIterator = db.execute(newFSEQuery, parameters).columnAs( "n" );
                        leafNode = resultIterator.next();
                        
                        creating = true;
                    }
                    else {
                        leafNode = null;
                        break;
                    }
                }
            }
            
            if (leafNode != null && ! creating) {
                leafNode.setProperty("path", pathSoFar);
            }
            
            return leafNode;
        }
        
        return null;
    }
    
    @GET
    @Path("/vrtrack_file_qc/{database}/{root}/{path}") 
    public Response vrtrackFileQC(@PathParam("database") String database,
                                           @PathParam("root") String root,
                                           @PathParam("path") String path,
                                           @Context GraphDatabaseService db) throws IOException {
        
        Label fseLabel = DynamicLabel.label(database + "|VRPipe|FileSystemElement");
        Label taxonLabel = DynamicLabel.label(database + "|VRTrack|Taxon");
        Label studyLabel = DynamicLabel.label(database + "|VRTrack|Study");
        boolean check_literal_props = false;
        boolean check_regex_props = false;
        List<List<String>> properties_literal = new ArrayList<List<String>>();
        List<Map.Entry<String,Pattern>> properties_regex = new ArrayList<>();
        
        HashMap<Long, HashMap<String, Object>> results = new HashMap<Long, HashMap<String, Object>>();
        try (Transaction tx = db.beginTx()) {
            Node leafNode = pathToFSE(db, database, fseLabel, root, path, false);
            
            if (leafNode != null) {
                addNodeDetailsToResults(leafNode, results, "FileSystemElement");
                
                // we're going to try and look for the closest lane/section, but
                // want to ignore these if they are via some bizzare route and
                // aren't for the closest sample, so first we get the closest
                // sample
                String sampleLabel = database + "|VRTrack|Sample";
                List<org.neo4j.graphdb.Path> paths = getClosestPaths(db, leafNode, sampleLabel, in, 20, 0, check_literal_props, check_regex_props, properties_literal, properties_regex);
                Node closestSample = null;
                org.neo4j.graphdb.Path pathToClosestSample = null;
                if (paths.size() == 1) {
                    pathToClosestSample = paths.get(0);
                    closestSample = pathToClosestSample.endNode();
                }
                
                // get the closest Lane or Section node, and the FSE that
                // has qc_file relationships
                Node fseWithQC = null;
                String laneOrSectionLabel = database + "|VRTrack|Lane";
                paths = getClosestPaths(db, leafNode, laneOrSectionLabel, in, 20, 0, check_literal_props, check_regex_props, properties_literal, properties_regex);
                if (paths.size() != 1) {
                    laneOrSectionLabel = database + "|VRTrack|Section";
                    paths = getClosestPaths(db, leafNode, laneOrSectionLabel, in, 20, 0, check_literal_props, check_regex_props, properties_literal, properties_regex);
                }
                if (paths.size() == 1) {
                    org.neo4j.graphdb.Path laneOrSectionPath = paths.get(0);
                    Node laneOrSection = laneOrSectionPath.endNode();
                    
                    // sanity check the sample
                    paths = getClosestPaths(db, laneOrSection, sampleLabel, in, 5, 0, check_literal_props, check_regex_props, properties_literal, properties_regex);
                    Node thisSample = null;
                    if (paths.size() == 1) {
                        org.neo4j.graphdb.Path pathToSample = paths.get(0);
                        thisSample = pathToSample.endNode();
                    }
                    
                    if (thisSample != null && thisSample.getId() == closestSample.getId()) {
                        addNodeDetailsToResults(laneOrSection, results, laneOrSectionLabel);
                        
                        // add all the hierarchy nodes
                        getSequencingHierarchy(laneOrSection, results, taxonLabel, studyLabel);
                        
                        // if there were multiple FSEs between leafNode and
                        // laneOrSection, we want to combine the properties of
                        // all of them on our single output FSE
                        Long leafNodeId = leafNode.getId();
                        HashMap<String, Object> fseProps = results.get(leafNodeId);
                        for (Node pathNode: laneOrSectionPath.reverseNodes()) {
                            if (pathNode.hasLabel(fseLabel)) {
                                if (pathNode.hasRelationship(VrtrackRelationshipTypes.qc_file, out)) {
                                    fseWithQC = pathNode;
                                }
                                
                                for (String property : pathNode.getPropertyKeys()) {
                                    fseProps.put(property, pathNode.getProperty(property));
                                }
                            }
                        }
                        
                        pathToClosestSample = null;
                    }
                }
                
                if (pathToClosestSample != null) {
                    // we could also be dealing with a sequenom|fluidgm
                    // result file that is directly attached to sample, or a
                    // array file that is related via a section;
                    // get the fse that is 1 or 2 away from closestSample
                    // so we can get the hierarchy from there
                    int count = 0;
                    boolean found = false;
                    Node prevNode = null;
                    Long leafNodeId = leafNode.getId();
                    HashMap<String, Object> fseProps = results.get(leafNodeId);
                    for (Node pathNode: pathToClosestSample.reverseNodes()) {
                        count++;
                        if (pathNode.hasLabel(fseLabel)) {
                            if (count == 2) {
                                if (pathNode.hasRelationship(VrtrackRelationshipTypes.processed, in)) {
                                    getSequencingHierarchy(pathNode, results, taxonLabel, studyLabel);
                                    found = true;
                                }
                            }
                            else if (count == 3 && ! found) {
                                if (pathNode.hasRelationship(VrtrackRelationshipTypes.processed, in)) {
                                    getSequencingHierarchy(prevNode, results, taxonLabel, studyLabel);
                                }
                            }
                            
                            for (String property : pathNode.getPropertyKeys()) {
                                fseProps.put(property, pathNode.getProperty(property));
                            }
                        }
                        else {
                            prevNode = pathNode;
                        }
                    }
                }
                
                if (fseWithQC == null) {
                    // we didn't find any file nodes with qc_file rels, but
                    // we'll still try and go ahead for the non-qc_file
                    // nodes
                    fseWithQC = leafNode;
                }
                
                HashMap<String, Integer> doneLabels = new HashMap<String, Integer>();
                qcFilesLoop: for (Relationship fqRel: fseWithQC.getRelationships(VrtrackRelationshipTypes.qc_file, out)) {
                    Node misc = fqRel.getEndNode();
                    
                    for (Map.Entry<RelationshipType, String> entry : fileQCTypes.entrySet()) {
                        String thisLabel = entry.getValue();
                        if (doneLabels.containsKey(thisLabel)) {
                            continue;
                        }
                        
                        RelationshipType rType = entry.getKey();
                        
                        int latest = 0;
                        Node latestNode = null;
                        for (Relationship rel: misc.getRelationships(rType, out)) {
                            Node thisNode = rel.getEndNode();
                            int thisDate = 0;
                            if (thisNode.hasProperty("date")) {
                                thisDate = Integer.parseInt(thisNode.getProperty("date").toString());
                            }
                            
                            if (latestNode == null || thisDate > latest) {
                                latestNode = thisNode;
                                latest = thisDate;
                            }
                        }
                        
                        if (latestNode != null) {
                            addNodeDetailsToResults(latestNode, results, thisLabel);
                            doneLabels.put(thisLabel, Integer.valueOf(1));
                            continue qcFilesLoop;
                        }
                    }
                }
                
                // Header_Mistakes nodes are directly attached to the file,
                // and don't have a date property, so we'll hope it's ok to
                // get the latest by node id
                Long latest = Long.valueOf(0);
                Node latestHeaderNode = null;
                for (Relationship fhRel: fseWithQC.getRelationships(VrtrackRelationshipTypes.header_mistakes, out)) {
                    Node header = fhRel.getEndNode();
                    Long nodeId = header.getId();
                    
                    if (nodeId > latest) {
                        latestHeaderNode = header;
                        latest = nodeId;
                    }
                }
                if (latestHeaderNode != null) {
                    addNodeDetailsToResults(latestHeaderNode, results, "Header_Mistakes");
                }
                
                // Auto_QC nodes are directly attached to the file and do
                // have a date property
                int latestAQC = 0;
                Node latestAQCNode = null;
                for (Relationship rel: fseWithQC.getRelationships(VrtrackRelationshipTypes.auto_qc_status, out)) {
                    Node thisNode = rel.getEndNode();
                    int thisDate = 0;
                    if (thisNode.hasProperty("date")) {
                        thisDate = Integer.parseInt(thisNode.getProperty("date").toString());
                    }
                    
                    if (latestAQCNode == null || thisDate > latestAQC) {
                        latestAQCNode = thisNode;
                        latestAQC = thisDate;
                    }
                }
                if (latestAQCNode != null) {
                    addNodeDetailsToResults(latestAQCNode, results, "Auto_QC");
                }
            }
        }
        
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    @GET
    @Path("/get_or_store_filesystem_paths/{database}/{root}/{paths}") 
    public Response fsePathsToNodes(@PathParam("database") String database,
                                    @PathParam("root") String root,
                                    @PathParam("paths") String pathsStr,
                                    @DefaultValue("0") @QueryParam("only_get") Integer only_get,
                                    @Context GraphDatabaseService db) throws IOException {
        Label fseLabel = DynamicLabel.label(database + "|VRPipe|FileSystemElement");
        boolean create = true;
        if (only_get == 1) {
            create = false;
        }
        String[] paths = pathsStr.split("///");
        
        HashMap<Long, HashMap<String, Object>> results = new HashMap<Long, HashMap<String, Object>>();
        try (Transaction tx = db.beginTx()) {
            for (String path: paths) {
                Node fse = pathToFSE(db, database, fseLabel, root, path, create);
                if (fse != null) {
                    addNodeDetailsToResults(fse, results, "FileSystemElement");
                }
            }
            
            if (create) {
                tx.success();
            }
        }
        
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    private String[] fseToPathAndRoot (Node leafNode) {
        Node origLeaf = leafNode;
        
        ArrayList<String> basenames = new ArrayList<String>();
        basenames.add(leafNode.getProperty("basename", "").toString());
        
        Relationship rel = leafNode.getSingleRelationship(VrtrackRelationshipTypes.contains, in);
        while (rel != null) {
            leafNode = rel.getStartNode();
            basenames.add(0, leafNode.getProperty("basename", "").toString());
            rel = leafNode.getSingleRelationship(VrtrackRelationshipTypes.contains, in);
        }
        
        String rootBasename = basenames.remove(0);
        String path = "/" + StringUtils.join(basenames, "/");
        
        if (origLeaf.hasProperty("path")) {
            String storedPath = origLeaf.getProperty("path").toString();
            if (! path.equals(storedPath)) {
                origLeaf.setProperty("path", path);
            }
        }
        
        String[] result = {path, rootBasename};
        return result;
    }
    
    @GET
    @Path("/filesystemelement_to_path/{id}") 
    public Response fseToPath(@PathParam("id") Long fseId,
                              @Context GraphDatabaseService db) throws IOException {
        
        HashMap<String, String> results = new HashMap<String, String>();
        try (Transaction tx = db.beginTx()) {
            Node leafNode;
            try {
                leafNode = db.getNodeById(fseId);
            }
            catch (NotFoundException e) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
            String[] pathAndRoot = fseToPathAndRoot(leafNode);
            
            results.put("path", pathAndRoot[0]);
            results.put("root", pathAndRoot[1]);
        }
        
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    private Object[] fseSourceToNode (GraphDatabaseService db, String sourceIdOrPath, String database, String sourceRoot, Label fseLabel) {
        Node sourceNode = null;
        
        String origSourcePath = "";
        if (sourceIdOrPath.matches("^\\d+$") && sourceRoot == null) {
            try {
                sourceNode = db.getNodeById(Long.parseLong(sourceIdOrPath));
            }
            catch (NotFoundException e) {
                return null;
            }
            
            String[] pathAndRoot = fseToPathAndRoot(sourceNode);
            origSourcePath = pathAndRoot[0];
            sourceRoot = pathAndRoot[1];
        }
        else if (sourceRoot != null) {
            sourceNode = pathToFSE(db, database, fseLabel, sourceRoot, sourceIdOrPath, false);
            origSourcePath = sourceIdOrPath;
        }
        
        Object[] result = new Object[]{sourceNode, origSourcePath, sourceRoot};
        return result;
    }
    
    @GET
    @Path("/filesystemelement_move/{database}/{sourceIdOrPath}/{destRoot}/{destDir}/{destBasename}") 
    public Response fseMove(@PathParam("database") String database,
                            @PathParam("sourceIdOrPath") String sourceIdOrPath,
                            @PathParam("destRoot") String destRoot,
                            @PathParam("destDir") String destDir,
                            @PathParam("destBasename") String destBasename,
                            @QueryParam("source_root") String sourceRoot,
                            @Context GraphDatabaseService db) throws IOException {
        
        Label fseLabel = DynamicLabel.label(database + "|VRPipe|FileSystemElement");
        
        HashMap<Long, HashMap<String, Object>> results = new HashMap<Long, HashMap<String, Object>>();
        try (Transaction tx = db.beginTx()) {
            Object[] fstnResult = fseSourceToNode(db, sourceIdOrPath, database, sourceRoot, fseLabel);
            Node sourceNode = (Node)fstnResult[0];
            String origSourcePath = (String)fstnResult[1];
            sourceRoot = (String)fstnResult[2];
            
            if (sourceNode == null || (sourceRoot.equals(destRoot) && origSourcePath.equals(destDir + "/" + destBasename))) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
            Node destDirNode = pathToFSE(db, database, fseLabel, destRoot, destDir, true);
            
            // remove existing contains rel pointing to source
            Relationship rel = sourceNode.getSingleRelationship(VrtrackRelationshipTypes.contains, in);
            rel.delete();
            
            // create a rel from destDir to source
            destDirNode.createRelationshipTo(sourceNode, VrtrackRelationshipTypes.contains);
            
            // update basename and path on source
            sourceNode.setProperty("path", destDir + "/" + destBasename);
            sourceNode.setProperty("basename", destBasename);
            
            // create a node at the original source location so that we can say
            // we were moved from there
            Node movedFromNode = pathToFSE(db, database, fseLabel, sourceRoot, origSourcePath, true);
            rel = sourceNode.getSingleRelationship(VrtrackRelationshipTypes.moved_from, out);
            if (rel != null) {
                rel.delete();
            }
            sourceNode.createRelationshipTo(movedFromNode, VrtrackRelationshipTypes.moved_from);
            
            // also transfer any moved_from rels from sourceNode to movedFromNode
            for (Relationship mrel : sourceNode.getRelationships(VrtrackRelationshipTypes.moved_from, in)) {
                Node movedNode = mrel.getStartNode();
                mrel.delete();
                movedNode.createRelationshipTo(movedFromNode, VrtrackRelationshipTypes.moved_from);
            }
            
            addNodeDetailsToResults(sourceNode, results, "FileSystemElement");
            
            tx.success();
        }
        
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    @GET
    @Path("/filesystemelement_duplicate/{database}/{sourceIdOrPath}/{destRoot}/{destPath}/{relation}") 
    public Response fseDup(@PathParam("database") String database,
                            @PathParam("sourceIdOrPath") String sourceIdOrPath,
                            @PathParam("destRoot") String destRoot,
                            @PathParam("destPath") String destPath,
                            @PathParam("relation") String relation,
                            @QueryParam("source_root") String sourceRoot,
                            @Context GraphDatabaseService db) throws IOException {
        
        Label fseLabel = DynamicLabel.label(database + "|VRPipe|FileSystemElement");
        
        HashMap<Long, HashMap<String, Object>> results = new HashMap<Long, HashMap<String, Object>>();
        
        RelationshipType relType = null;
        switch (relation.toLowerCase()) {
            case "symlink":
                relType = VrtrackRelationshipTypes.symlink;
                break;
            case "copy":
                relType = VrtrackRelationshipTypes.copy;
                break;
            default:
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
        }
        
        try (Transaction tx = db.beginTx()) {
            Object[] fstnResult = fseSourceToNode(db, sourceIdOrPath, database, sourceRoot, fseLabel);
            Node sourceNode = (Node)fstnResult[0];
            String origSourcePath = (String)fstnResult[1];
            sourceRoot = (String)fstnResult[2];
            
            if (sourceNode == null || (sourceRoot.equals(destRoot) && origSourcePath.equals(destPath))) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
            Node dupNode = pathToFSE(db, database, fseLabel, destRoot, destPath, true);
            
            Relationship oldRel = dupNode.getSingleRelationship(relType, in);
            if (oldRel != null) {
                oldRel.delete();
            }
            sourceNode.createRelationshipTo(dupNode, relType);
            
            addNodeDetailsToResults(dupNode, results, "FileSystemElement");
            
            tx.success();
        }
        
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    @GET
    @Path("/filesystemelement_parent/{database}/{sourceIdOrPath}") 
    public Response fseDup(@PathParam("database") String database,
                            @PathParam("sourceIdOrPath") String sourceIdOrPath,
                            @QueryParam("source_root") String sourceRoot,
                            @Context GraphDatabaseService db) throws IOException {
        
        Label fseLabel = DynamicLabel.label(database + "|VRPipe|FileSystemElement");
        
        HashMap<Long, HashMap<String, Object>> results = new HashMap<Long, HashMap<String, Object>>();
        try (Transaction tx = db.beginTx()) {
            Object[] fstnResult = fseSourceToNode(db, sourceIdOrPath, database, sourceRoot, fseLabel);
            Node sourceNode = (Node)fstnResult[0];
            
            if (sourceNode == null) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
            // follow back along symlink or copy rels to earliest fse
            Node parent = sourceNode;
            
            Relationship rel = parent.getSingleRelationship(VrtrackRelationshipTypes.symlink, in);
            if (rel == null) {
                rel = parent.getSingleRelationship(VrtrackRelationshipTypes.copy, in);
            }
            while (rel != null) {
                parent = rel.getStartNode();
                rel = parent.getSingleRelationship(VrtrackRelationshipTypes.symlink, in);
                if (rel == null) {
                    rel = parent.getSingleRelationship(VrtrackRelationshipTypes.copy, in);
                }
            }
            
            addNodeDetailsToResults(parent, results, "FileSystemElement");
        }
        
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    /*
        For the graph_vrtrack datasource we need to quickly get crams under
        a point in the VRTrack hierarchy along with all hierarchy info, and
        we implement here for speed instead of calling the
        get_sequencing_hierarchy service many times externally, which is slow
    */
    
    private void addHierarchyNodeProps (Node node, String label, HashMap<String, String> props) {
        for (String pKey: node.getPropertyKeys()) {
            Object val = node.getProperty(pKey);
            if (val instanceof String) {
                String valStr = (String) val;
                if (label.equals("FileSystemElement")) {
                    props.put(pKey, valStr);
                }
                else {
                    props.put(label + "_" + pKey, valStr);
                }
            }
        }
    }
    
    private boolean passesVRTrackFilesParentFilter (HashMap<String, HashMap<String, ArrayList<String>>> parentFilters, String label, HashMap<String, String> hierarchyProps) {
        HashMap<String, ArrayList<String>> labelMap = parentFilters.get(label);
        if (labelMap != null) {
            for (Map.Entry<String, ArrayList<String>> entry : labelMap.entrySet()) {
                String property = entry.getKey();
                ArrayList<String> vals = entry.getValue();
                String actualVal = hierarchyProps.get(label + "_" + property);
                
                int ok = 0;
                for (String val: vals) {
                    if (actualVal == null && (val == null || val.equals("0"))) {
                        // allow a desired 0 to match an unspecified
                        // node property
                        ok++;
                    }
                    else if (actualVal != null && actualVal.equals(val)) {
                        ok++;
                    }
                }
                
                if (ok == 0) {
                    return false;
                }
            }
        }
        return true;
    }
    
    private boolean passesVRTrackFilesQCFilter (HashMap<String, ArrayList<String[]>> qcFilters, Node fileNode, HashMap<String, String> qcProps) {
        // unlike vrtrack_file_qc, fileNode must be directly attached to lane
        // and have qc_file realtionships - we're not going to search for a
        // better fse. Instead we immediately try and find the desired qc nodes
        // that have the values we need to filter on
        HashMap<Long, Integer> doneQCNodes = new HashMap<Long, Integer>();
        for (Map.Entry<String, ArrayList<String[]>> entry : qcFilters.entrySet()) {
            String type = entry.getKey();
            Node qcNode = null;
            
            switch (type) {
                case "file":
                    qcNode = fileNode;
                    break;
                case "header_mistakes":
                    Long latest = Long.valueOf(0);
                    Node latestHeaderNode = null;
                    for (Relationship fhRel: fileNode.getRelationships(VrtrackRelationshipTypes.header_mistakes, out)) {
                        Node header = fhRel.getEndNode();
                        Long nodeId = header.getId();
                        
                        if (nodeId > latest) {
                            latestHeaderNode = header;
                            latest = nodeId;
                        }
                    }
                    if (latestHeaderNode != null) {
                        qcNode = latestHeaderNode;
                    }
                    else {
                        return false;
                    }
                    break;
                case "auto_qc":
                    int latestAQC = 0;
                    Node latestAQCNode = null;
                    for (Relationship rel: fileNode.getRelationships(VrtrackRelationshipTypes.auto_qc_status, out)) {
                        Node thisNode = rel.getEndNode();
                        int thisDate = 0;
                        if (thisNode.hasProperty("date")) {
                            thisDate = Integer.parseInt(thisNode.getProperty("date").toString());
                        }
                        
                        if (latestAQCNode == null || thisDate > latestAQC) {
                            latestAQCNode = thisNode;
                            latestAQC = thisDate;
                        }
                    }
                    if (latestAQCNode != null) {
                        qcNode = latestAQCNode;
                    }
                    else {
                        return false;
                    }
                    break;
                default:
                    // fileNode has a number of qc_file rels that point to an
                    // fse that points to a possible node of interest
                    RelationshipType qcRelType = null;
                    switch (type) {
                        case "genotype":
                            qcRelType = VrtrackRelationshipTypes.genotype_data;
                            break;
                        case "stats":
                            qcRelType = VrtrackRelationshipTypes.summary_stats;
                            break;
                        case "verifybamid":
                            qcRelType = VrtrackRelationshipTypes.verify_bam_id_data;
                            break;
                        default:
                            return false;
                    }
                    
                    for (Relationship fqRel: fileNode.getRelationships(VrtrackRelationshipTypes.qc_file, out)) {
                        Node qcFile = fqRel.getEndNode();
                        if (doneQCNodes.containsKey(qcFile.getId())) {
                            continue;
                        }
                        
                        int latestDate = 0;
                        Node latestNode = null;
                        for (Relationship rel: qcFile.getRelationships(qcRelType, out)) {
                            Node thisNode = rel.getEndNode();
                            int thisDate = 0;
                            if (thisNode.hasProperty("date")) {
                                thisDate = Integer.parseInt(thisNode.getProperty("date").toString());
                            }
                            
                            if (latestNode == null || thisDate > latestDate) {
                                latestNode = thisNode;
                                latestDate = thisDate;
                            }
                        }
                        
                        if (latestNode != null) {
                            qcNode = latestNode;
                            doneQCNodes.put(qcFile.getId(), Integer.valueOf(1));
                        }
                    }
                    
                    if (qcNode == null) {
                        return false;
                    }
            }
            
            ArrayList<String[]> filterList = entry.getValue();
            for (String[] filterVals : filterList) {
                String property = filterVals[0];
                String operator = filterVals[1];
                String value = filterVals[2];
                
                Object gpObj = qcNode.getProperty(property, null);
                
                if (gpObj == null) {
                    if (value.equals("0") && operator.equals("==")) {
                        // allow 0 to match undef
                        qcProps.put(type + "_" + property, "0");
                        continue;
                    }
                    else {
                        return false;
                    }
                }
                
                String actualVal = gpObj.toString();
                
                switch (operator) {
                    case "==":
                        if (! actualVal.equals(value)) {
                            return false;
                        }
                        break;
                    case "!=":
                        if (actualVal.equals(value)) {
                            return false;
                        }
                        break;
                    default:
                        int actualInt = Integer.parseInt(actualVal);
                        int desiredInt = Integer.parseInt(value);
                        
                        switch (operator) {
                            case "<":
                                if (! (actualInt < desiredInt)) {
                                    return false;
                                }
                                break;
                            case "<=":
                                if (! (actualInt <= desiredInt)) {
                                    return false;
                                }
                                break;
                            case ">":
                                if (! (actualInt > desiredInt)) {
                                    return false;
                                }
                                break;
                            case ">=":
                                if (! (actualInt >= desiredInt)) {
                                    return false;
                                }
                                break;
                            default:
                                return false;
                        }
                }
                
                if (! type.equals("file")) {
                    qcProps.put(type + "_" + property, actualVal);
                }
            }
        }
        
        return true;
    }
    
    @GET
    @Path("/vrtrack_alignment_files/{database}/{source}/{fileext}") 
    public Response getVRTrackFiles(@PathParam("database") String database,
                                    @PathParam("source") String source,
                                    @PathParam("fileext") String fileExt,
                                    @QueryParam("parent_filter") String pf,
                                    @QueryParam("qc_filter") String qf,
                                    @Context GraphDatabaseService db) throws IOException {
        
        Label fseLabel = DynamicLabel.label(database + "|VRPipe|FileSystemElement");
        String laneLabelStr = database + "|VRTrack|Lane";
        Label laneLabel = DynamicLabel.label(laneLabelStr);
        Label libLabel = DynamicLabel.label(database + "|VRTrack|Library");
        Label taxonLabel = DynamicLabel.label(database + "|VRTrack|Taxon");
        Label studyLabel = DynamicLabel.label(database + "|VRTrack|Study");
        
        // Source is a description of a parent node(s) of your desired Lanes, in
        // the form 'Label#property#value1,value2'. Eg. 'Study#id#123,456,789'
        // to work with all lanes under studies with those 3 ids.
        String[] sourceSplit = source.split("#");
        Label sourceLabel = DynamicLabel.label(database + "|VRTrack|" + sourceSplit[0]);
        String sourceProperty = sourceSplit[1];
        String[] sourceVals = sourceSplit[2].split(",");
        List<List<String>> properties_literal = new ArrayList<List<String>>();
        List<Map.Entry<String,Pattern>> properties_regex = new ArrayList<>();
        
        // The parent_filter option is a string of the form
        // 'Label#propery#value'; multiple filters can be separated by commas
        // (and having the same Label and property multiple times with different
        // values means the actual value must match one of those values). The
        // filter will look for an exact match to a property of a node that the
        // file's node is descended from, eg. specify Sample#qc_failed#0 to only
        // have files related to samples that have not been qc failed.
        HashMap<String, HashMap<String, ArrayList<String>>> parentFilters = new HashMap<String, HashMap<String, ArrayList<String>>>();
        if (pf != null) {
            // Sample#qc_failed#0,Sample#created_date#1426150152
            for (String filter: pf.split(",")) {
                String[] parts = filter.split("#");
                String label = parts[0].toLowerCase();
                
                HashMap<String, ArrayList<String>> labelMap = parentFilters.get(label);
                if (labelMap == null) {
                    labelMap = new HashMap<String, ArrayList<String>>();
                    parentFilters.put(label, labelMap);
                }
                
                ArrayList<String> propVals = labelMap.get(parts[1]);
                if (propVals == null) {
                    propVals = new ArrayList<String>();
                    labelMap.put(parts[1], propVals);
                }
                
                propVals.add(parts[2]);
            }
        }
        
        // The qc_filter option lets you filter on properties of the file itself
        // or on properties of certain qc-related nodes that are children of the
        // file and may be created by some downstream analysis; these are
        // specified in the form 'psuedoLabel#property#operator#value', and
        // multiple of these can be separated by commas. An example might be
        // 'stats#sequences#>#10000,stats#reads QC failed#<#1000,genotype#pass#=
        // #1,verifybamid#pass#=#1,file#manual_qc#=#1,file#vrtrack_qc_passed#=#1
        // ' to only use cram files with more than 10000 total sequences and
        // fewer than 1000 qc failed reads (according to the Bam_Stats node),
        // with a genotype status of 'pass' (from the Genotype node), a 'pass'
        // from the verify bam id process (from the Verify_Bam_ID node) and with
        // manual_qc and vrtrack_qc_passed metadata set to 1 on the node
        // representing the cram file itself.
        boolean doQCFiltering = false;
        HashMap<String, ArrayList<String[]>> qcFilters = new HashMap<String, ArrayList<String[]>>();
        if (qf != null) {
            for (String filter: qf.split(",")) {
                String[] parts = filter.split("#");
                String type = parts[0].toLowerCase();
                String property = parts[1];
                String operator = parts[2];
                String value = parts[3];
                
                if (operator.equals("=")) {
                    operator = "==";
                }
                
                ArrayList<String[]> filterList = qcFilters.get(type);
                if (filterList == null) {
                    filterList = new ArrayList<String[]>();
                    qcFilters.put(type, filterList);
                }
                
                String[] filterVals = { property, operator, value };
                filterList.add(filterVals);
                doQCFiltering = true;
            }
        }
        
        HashMap<String, HashMap<String, HashMap<String, String>>> results = new HashMap<String, HashMap<String, HashMap<String, String>>>();
        try (Transaction tx = db.beginTx()) {
            ArrayList<Node> lanes = new ArrayList<Node>();
            for (String sourceVal: sourceVals) {
                Node vrtrackNode = db.findNode(sourceLabel, sourceProperty, sourceVal);
                if (vrtrackNode != null) {
                    switch (sourceSplit[0]) {
                        case "Lane":
                            lanes.add(vrtrackNode);
                            break;
                        case "Study":
                            for (Relationship srel : vrtrackNode.getRelationships(VrtrackRelationshipTypes.created_for, in)) {
                                Node lane = srel.getStartNode();
                                if (lane.hasLabel(laneLabel)) {
                                    lanes.add(lane);
                                }
                            }
                            break;
                        case "Sample":
                            for (Relationship srel : vrtrackNode.getRelationships(VrtrackRelationshipTypes.prepared, out)) {
                                Node lib = srel.getEndNode();
                                if (lib.hasLabel(libLabel)) {
                                    for (Relationship lrel : lib.getRelationships(VrtrackRelationshipTypes.sequenced, out)) {
                                        Node lane = lrel.getEndNode();
                                        if (lane.hasLabel(laneLabel)) {
                                            lanes.add(lane);
                                        }
                                    }
                                }
                            }
                            break;
                        case "Library":
                            for (Relationship lrel : vrtrackNode.getRelationships(VrtrackRelationshipTypes.sequenced, out)) {
                                Node lane = lrel.getEndNode();
                                if (lane.hasLabel(laneLabel)) {
                                    lanes.add(lane);
                                }
                            }
                            break;
                        default:
                            List<Node> closeNodes = getClosestNodes(db, vrtrackNode, laneLabelStr, out, 20, 1, false, false, properties_literal, properties_regex);
                            for (Node node : closeNodes) {
                                lanes.add(node);
                            }
                    }
                }
            }
            
            if (! (lanes.size() > 0)) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
            int passed = 0;
            int failedPF = 0;
            int failedQF = 0;
            int failedNoCramFile = 0;
            LANE: for (Node lane: lanes) {
                Node alignFile = null;
                for (Relationship lrel : lane.getRelationships(VrtrackRelationshipTypes.aligned, out)) {
                    Node fse = lrel.getEndNode();
                    if (fse.hasLabel(fseLabel) && fse.getProperty("basename").toString().endsWith(fileExt)) {
                        alignFile = fse;
                        break;
                    }
                }
                
                if (alignFile == null) {
                    failedNoCramFile++;
                    continue;
                }
                
                
                // get props of file and store them in result under path of
                // this file
                HashMap<String, HashMap<String, String>> fileResult = new HashMap<String, HashMap<String, String>>();
                
                // first check it passes any qc filters, and store the qc
                // properties we filtered on
                if (doQCFiltering) {
                    HashMap<String, String> qcProps = new HashMap<String, String>();
                    if (! passesVRTrackFilesQCFilter(qcFilters, alignFile, qcProps)) {
                        failedQF++;
                        continue;
                    }
                    
                    // if the only filter was on file, we won't have any
                    // qcProps, since we get all file props anyway below
                    if (! qcProps.isEmpty()) {
                        fileResult.put("qc_meta", qcProps);
                    }
                }
                
                HashMap<String, String> fileProps = new HashMap<String, String>();
                addHierarchyNodeProps(alignFile, "FileSystemElement", fileProps);
                String alignPath = fileProps.remove("path");
                fileProps.put("node_id", String.valueOf(alignFile.getId()));
                fileResult.put("properties", fileProps);
                
                // walk up the hierarchy and store hierarchy info
                HashMap<String, String> hierarchyProps = new HashMap<String, String>();
                addHierarchyNodeProps(lane, "lane", hierarchyProps);
                
                // if there's a parent filter on lanes, filter on that now
                if (! passesVRTrackFilesParentFilter(parentFilters, "lane", hierarchyProps)) {
                    failedPF++;
                    continue;
                }
                
                Relationship rel = lane.getSingleRelationship(VrtrackRelationshipTypes.sequenced, in);
                if (rel != null) {
                    Node lib = rel.getStartNode();
                    addHierarchyNodeProps(lib, "library", hierarchyProps);
                    if (! passesVRTrackFilesParentFilter(parentFilters, "library", hierarchyProps)) {
                        failedPF++;
                        continue;
                    }
                    
                    rel = lib.getSingleRelationship(VrtrackRelationshipTypes.prepared, in);
                    if (rel != null) {
                        Node sample = rel.getStartNode();
                        addHierarchyNodeProps(sample, "sample", hierarchyProps);
                        if (! passesVRTrackFilesParentFilter(parentFilters, "sample", hierarchyProps)) {
                            failedPF++;
                            continue;
                        }
                        
                        for (Relationship grel : sample.getRelationships(VrtrackRelationshipTypes.gender, out)) {
                            Node gender = grel.getEndNode();
                            addHierarchyNodeProps(gender, "gender", hierarchyProps);
                            if (! passesVRTrackFilesParentFilter(parentFilters, "gender", hierarchyProps)) {
                                failedPF++;
                                continue LANE;
                            }
                            break;
                        }
                        
                        rel = sample.getSingleRelationship(VrtrackRelationshipTypes.sample, in);
                        if (rel != null) {
                            Node donor = rel.getStartNode();
                            addHierarchyNodeProps(donor, "donor", hierarchyProps);
                            if (! passesVRTrackFilesParentFilter(parentFilters, "donor", hierarchyProps)) {
                                failedPF++;
                                continue;
                            }
                        }
                        
                        Node directlyAttachedStudy = null;
                        rel = lane.getSingleRelationship(VrtrackRelationshipTypes.created_for, out);
                        if (rel != null) {
                            directlyAttachedStudy = rel.getEndNode();
                            addHierarchyNodeProps(directlyAttachedStudy, "study", hierarchyProps);
                        }
                        
                        Node preferredStudy = null;
                        Node anyStudy = null;
                        for (Relationship mrel : sample.getRelationships(VrtrackRelationshipTypes.member, in)) {
                            Node parent = mrel.getStartNode();
                            
                            if (parent.hasLabel(taxonLabel)) {
                                addHierarchyNodeProps(parent, "taxon", hierarchyProps);
                                if (! passesVRTrackFilesParentFilter(parentFilters, "taxon", hierarchyProps)) {
                                    failedPF++;
                                    continue LANE;
                                }
                            }
                            else if (directlyAttachedStudy == null && parent.hasLabel(studyLabel)) {
                                if (preferredStudy == null && mrel.hasProperty("preferred")) {
                                    String pref = mrel.getProperty("preferred").toString();
                                    if (pref.equals("1")) {
                                        preferredStudy = parent;
                                        addHierarchyNodeProps(preferredStudy, "study", hierarchyProps);
                                    }
                                }
                                else if (anyStudy == null) {
                                    anyStudy = parent;
                                }
                            }
                        }
                        
                        if (preferredStudy == null && anyStudy != null) {
                            addHierarchyNodeProps(anyStudy, "study", hierarchyProps);
                        }
                        
                        if (! passesVRTrackFilesParentFilter(parentFilters, "study", hierarchyProps)) {
                            failedPF++;
                            continue;
                        }
                    }
                }
                fileResult.put("hierarchy", hierarchyProps);
                
                results.put(alignPath, fileResult);
                passed++;
            }
            
            // we have a fake "file" in the results that lets us return stats
            // about how search went
            HashMap<String, HashMap<String, String>> statsResult = new HashMap<String, HashMap<String, String>>();
            HashMap<String, String> statProps = new HashMap<String, String>();
            statProps.put("passed", String.valueOf(passed));
            statProps.put("failed parent filter", String.valueOf(failedPF));
            statProps.put("failed qc filter", String.valueOf(failedQF));
            statProps.put("failed no cram file", String.valueOf(failedNoCramFile));
            statsResult.put("stats", statProps);
            results.put("search", statsResult);
        }
        
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
}
