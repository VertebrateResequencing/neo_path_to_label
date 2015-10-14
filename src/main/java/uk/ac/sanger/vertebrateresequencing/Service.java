package uk.ac.sanger.vertebrateresequencing;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

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
        sequenced, prepared, gender, member, sample, placed, section, has,
        administers, failed_by, selected_by, passed_by, processed, imported,
        converted, discordance,
        cnv_calls, loh_calls, copy_number_by_chromosome_plot, cnv_plot,
        pluritest, pluritest_plot
    }
    
    Direction in = Direction.INCOMING;
    Direction out = Direction.OUTGOING;
    
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
        
        LabelEvaluator labelEvaluator = new LabelEvaluator(DynamicLabel.label(label), id);
        PathExpander pathExpander = PathExpanderBuilder.allTypes(direction).build();
        
        TraversalDescription td = db.traversalDescription()
                .breadthFirst()
                .evaluator(labelEvaluator)
                .evaluator(Evaluators.toDepth(depth))
                .expand(pathExpander)
                .uniqueness(Uniqueness.NODE_GLOBAL);
        
        HashMap<Long, HashMap<String, Object>> results = new HashMap<Long, HashMap<String, Object>>();
        try (Transaction tx = db.beginTx()) {
            Node start;
            try {
                start = db.getNodeById(id);
            }
            catch (NotFoundException e) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
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
            
            tx.success();
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
            Node lane;
            try {
                lane = db.getNodeById(id);
            }
            catch (NotFoundException e) {
                return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
            }
            
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
                }
                else {
                    return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
                }
                
                rel = lane.getSingleRelationship(VrtrackRelationshipTypes.placed, in);
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
            
            for (Relationship grel : sample.getRelationships(VrtrackRelationshipTypes.gender, out)) {
                Node gender = grel.getEndNode();
                addNodeDetailsToResults(gender, results, "Gender");
            }
            
            rel = sample.getSingleRelationship(VrtrackRelationshipTypes.sample, in);
            if (rel != null) {
                Node donor = rel.getStartNode();
                addNodeDetailsToResults(donor, results, "Donor");
            }
            
            Node preferredStudy = null;
            Node anyStudy = null;
            for (Relationship mrel : sample.getRelationships(VrtrackRelationshipTypes.member, in)) {
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
                    }
                    else if (anyStudy == null) {
                        anyStudy = parent;
                    }
                }
            }
            
            if (preferredStudy == null && anyStudy != null) {
                addNodeDetailsToResults(anyStudy, results, "Study");
            }
            
            tx.success();
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
    public Response getSequencingHierarchy(@PathParam("database") String database,
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
            else if (statusToSet.equals("passed") || statusToSet.equals("selected") || statusToSet.equals("pending")) {
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
                    if (statusToSet.equals("failed")) {
                        sample.setProperty("qc_failed", "1");
                        sample.setProperty("qc_selected", "0");
                        sample.setProperty("qc_passed", "0");
                        suRelType = VrtrackRelationshipTypes.failed_by;
                    }
                    else if (statusToSet.equals("selected")) {
                        sample.setProperty("qc_failed", "0");
                        sample.setProperty("qc_selected", "1");
                        sample.setProperty("qc_passed", "0");
                        suRelType = VrtrackRelationshipTypes.selected_by;
                    }
                    else if (statusToSet.equals("passed")) {
                        sample.setProperty("qc_failed", "0");
                        sample.setProperty("qc_selected", "0");
                        sample.setProperty("qc_passed", "1");
                        suRelType = VrtrackRelationshipTypes.passed_by;
                    }
                    else if (statusToSet.equals("pending")) {
                        sample.setProperty("qc_failed", "0");
                        sample.setProperty("qc_selected", "0");
                        sample.setProperty("qc_passed", "0");
                    }
                    
                    // remove any existing rels
                    Relationship qcRel = sample.getSingleRelationship(VrtrackRelationshipTypes.failed_by, out);
                    if (qcRel != null) {
                        qcRel.delete();
                    }
                    qcRel = sample.getSingleRelationship(VrtrackRelationshipTypes.selected_by, out);
                    if (qcRel != null) {
                        qcRel.delete();
                    }
                    qcRel = sample.getSingleRelationship(VrtrackRelationshipTypes.passed_by, out);
                    if (qcRel != null) {
                        qcRel.delete();
                    }
                    
                    // now say who did this qc status change, when and why
                    if (suRelType != null) {
                        qcRel = sample.createRelationshipTo(adminUser, suRelType);
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
                        qcVal = sample.getProperty("qc_passed", null);
                        if (qcVal != null && qcVal.equals("1")) {
                            qcStatus = "passed";
                            qcRel = sample.getSingleRelationship(VrtrackRelationshipTypes.passed_by, out);
                        }
                    }
                }
                sampleInfo.put("qc_status", qcStatus);
                
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
            
            // work out the largest study and keep the public names of samples
            // in that study
            String largestStudy = null;
            int largestStudyCount = 0;
            HashMap<String, Integer> largestStudySamplePublicNames = null;
            for (Map.Entry<String, List<Map.Entry<Node,HashMap<String, Object>>>> entry : studyToSampleInfo.entrySet()) {
                int size = entry.getValue().size();
                if (size > largestStudyCount) {
                    largestStudy = entry.getKey();
                    largestStudyCount = size;
                    
                    largestStudySamplePublicNames = new HashMap<String, Integer>();
                    for (Map.Entry<Node,HashMap<String, Object>> nodeAndInfo : entry.getValue()) {
                        HashMap<String, Object> sampleInfo = nodeAndInfo.getValue();
                        if (! sampleInfo.get("qc_status").equals("failed")) {
                            largestStudySamplePublicNames.put(sampleInfo.get("public_name").toString(), Integer.valueOf(1));
                        }
                    }
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
                    
                    // a sample can be in multiple studies, but we don't process
                    // them more than once
                    String sampleName = sampleInfo.get("name").toString();
                    if (doneSamples.containsKey(sampleName)) {
                        continue;
                    }
                    else {
                        doneSamples.put(sampleName, Integer.valueOf(1));
                    }
                    
                    // we'll get the latest genotype node for all samples,
                    // and the latest fluidigm node for samples in the
                    // largest study, and samples that share a public_name with
                    // samples in the largest study
                    Node sample = nodeAndInfo.getKey();
                    boolean getFluidigm = false;
                    if (thisStudy.equals(largestStudy) || largestStudySamplePublicNames.containsKey(sampleInfo.get("public_name"))) {
                        getFluidigm = true;
                    }
                    
                    
                    HashMap<String, Node> discs = new HashMap<String, Node>();
                    int genotypeDiscEpoch = 0;
                    int fluidigmDiscEpoch = 0;
                    for (Relationship sdRel : sample.getRelationships(VrtrackRelationshipTypes.discordance, out)) {
                        Node disc = sdRel.getEndNode();
                        if (disc.getProperty("type").equals("genotype")) {
                            int thisEpoch = Integer.parseInt(disc.getProperty("date").toString());
                            if (thisEpoch > genotypeDiscEpoch) {
                                discs.put("discordance_genotype", disc);
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
            
            tx.success();
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
}
