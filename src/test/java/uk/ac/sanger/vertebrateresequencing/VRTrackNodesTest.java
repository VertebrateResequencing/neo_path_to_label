package uk.ac.sanger.vertebrateresequencing;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class VRTrackNodesTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);
    
    @Test
    public void shouldGetGroupNodes() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Group").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("name", "g1");
        prop.put("neo4j_label", "Group");
        expected.put("0", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "g2");
        prop.put("neo4j_label", "Group");
        expected.put("1", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "g3");
        prop.put("neo4j_label", "Group");
        expected.put("2", prop);
        
        assertEquals(expected, actual);
    }
    
    @Test
    public void shouldGetStudyNodes() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Study").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("id", "1");
        prop.put("neo4j_label", "Study");
        expected.put("3", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("id", "2");
        prop.put("neo4j_label", "Study");
        expected.put("4", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("id", "3");
        prop.put("neo4j_label", "Study");
        expected.put("5", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("id", "4");
        prop.put("neo4j_label", "Study");
        expected.put("6", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("id", "5");
        prop.put("neo4j_label", "Study");
        expected.put("7", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("id", "6");
        prop.put("neo4j_label", "Study");
        expected.put("8", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Study?groups=1,2").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("id", "2");
        prop.put("neo4j_label", "Study");
        expected.put("4", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("id", "3");
        prop.put("neo4j_label", "Study");
        expected.put("5", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("id", "4");
        prop.put("neo4j_label", "Study");
        expected.put("6", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("id", "5");
        prop.put("neo4j_label", "Study");
        expected.put("7", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("id", "6");
        prop.put("neo4j_label", "Study");
        expected.put("8", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Study?groups=0").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("id", "1");
        prop.put("neo4j_label", "Study");
        expected.put("3", prop);
        
        assertEquals(expected, actual);
    }
    
    @Test
    public void shouldGetDonorNodes() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Donor").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> propd1 = new LinkedHashMap<String, Object>();
        propd1.put("id", "d1");
        propd1.put("example_sample", "s1");
        propd1.put("neo4j_label", "Donor");
        expected.put("9", propd1);
        LinkedHashMap<String, Object> propd2 = new LinkedHashMap<String, Object>();
        propd2.put("id", "d2");
        propd2.put("example_sample", "s4");
        propd2.put("neo4j_label", "Donor");
        expected.put("10", propd2);
        LinkedHashMap<String, Object> propd3 = new LinkedHashMap<String, Object>();
        propd3.put("id", "d3");
        propd3.put("example_sample", "s5");
        propd3.put("neo4j_label", "Donor");
        expected.put("11", propd3);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Donor?groups=2").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Donor?groups=2&studies=5").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        expected.put("9", propd1);
        expected.put("11", propd3);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Donor?groups=2&studies=4").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Donor?groups=2&studies=5&donors=11").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        expected.put("11", propd3);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Donor?groups=2&studies=5&samples=17").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        expected.put("11", propd3);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Donor?groups=2&studies=5&donors=11&samples=16").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        expected.put("11", propd3);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Donor?groups=2&studies=5&donors=9&samples=16").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Donor?groups=2&studies=5&donors=9,11&samples=16").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        expected.put("11", propd3);
        assertEquals(expected, actual);
    }
    
    @Test
    public void shouldGetSampleNodes() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Sample").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> props1 = new LinkedHashMap<String, Object>();
        props1.put("name", "s1");
        props1.put("study_id", "3");
        props1.put("study_node_id", "5");
        props1.put("donor_id", "d1");
        props1.put("donor_node_id", "9");
        props1.put("control", "1");
        props1.put("neo4j_label", "Sample");
        expected.put("12", props1);
        LinkedHashMap<String, Object> props2 = new LinkedHashMap<String, Object>();
        props2.put("name", "s2");
        props2.put("study_id", "3");
        props2.put("study_node_id", "5");
        props2.put("donor_id", "d1");
        props2.put("donor_node_id", "9");
        props2.put("neo4j_label", "Sample");
        expected.put("13", props2);
        LinkedHashMap<String, Object> props3 = new LinkedHashMap<String, Object>();
        props3.put("name", "s3");
        props3.put("study_id", "2");
        props3.put("study_node_id", "4");
        props3.put("donor_id", "d2");
        props3.put("donor_node_id", "10");
        props3.put("neo4j_label", "Sample");
        expected.put("14", props3);
        LinkedHashMap<String, Object> props4 = new LinkedHashMap<String, Object>();
        props4.put("name", "s4");
        props4.put("study_id", "2");
        props4.put("study_node_id", "4");
        props4.put("donor_id", "d2");
        props4.put("donor_node_id", "10");
        props4.put("control", "1");
        props4.put("neo4j_label", "Sample");
        expected.put("15", props4);
        LinkedHashMap<String, Object> props5 = new LinkedHashMap<String, Object>();
        props5.put("name", "s5");
        props5.put("study_id", "3");
        props5.put("study_node_id", "5");
        props5.put("donor_id", "d3");
        props5.put("donor_node_id", "11");
        props5.put("control", "1");
        props5.put("neo4j_label", "Sample");
        expected.put("16", props5);
        LinkedHashMap<String, Object> props6 = new LinkedHashMap<String, Object>();
        props6.put("name", "s6");
        props6.put("study_id", "3");
        props6.put("study_node_id", "5");
        props6.put("donor_id", "d3");
        props6.put("donor_node_id", "11");
        props6.put("neo4j_label", "Sample");
        expected.put("17", props6);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Sample?groups=2").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Sample?groups=2&studies=5").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        expected.put("12", props1);
        expected.put("13", props2);
        expected.put("16", props5);
        expected.put("17", props6);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Sample?groups=2&studies=4").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Sample?groups=2&studies=5&donors=11").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        expected.put("16", props5);
        expected.put("17", props6);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Sample?groups=2&studies=5&samples=17").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        expected.put("17", props6);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Sample?groups=2&studies=5&donors=11&samples=16").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        expected.put("16", props5);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Sample?groups=2&studies=5&donors=9&samples=16").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_nodes/vdp/Sample?groups=2&studies=5&donors=9,11&samples=16").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        expected.put("16", props5);
        assertEquals(expected, actual);
    }
    
    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (g1:`vdp|VRTrack|Group` {name:'g1'})")
                    .append("CREATE (g2:`vdp|VRTrack|Group` {name:'g2'})")
                    .append("CREATE (g3:`vdp|VRTrack|Group` {name:'g3'})")
                    .append("CREATE (stu1:`vdp|VRTrack|Study` {id:'1'})")
                    .append("CREATE (stu2:`vdp|VRTrack|Study` {id:'2'})")
                    .append("CREATE (stu3:`vdp|VRTrack|Study` {id:'3'})")
                    .append("CREATE (stu4:`vdp|VRTrack|Study` {id:'4'})")
                    .append("CREATE (stu5:`vdp|VRTrack|Study` {id:'5'})")
                    .append("CREATE (stu6:`vdp|VRTrack|Study` {id:'6'})")
                    .append("CREATE (d1:`vdp|VRTrack|Donor` {id:'d1'})")
                    .append("CREATE (d2:`vdp|VRTrack|Donor` {id:'d2'})")
                    .append("CREATE (d3:`vdp|VRTrack|Donor` {id:'d3'})")
                    .append("CREATE (s1:`vdp|VRTrack|Sample` {name:'s1',control:'1'})")
                    .append("CREATE (s2:`vdp|VRTrack|Sample` {name:'s2'})")
                    .append("CREATE (s3:`vdp|VRTrack|Sample` {name:'s3'})")
                    .append("CREATE (s4:`vdp|VRTrack|Sample` {name:'s4',control:'1'})")
                    .append("CREATE (s5:`vdp|VRTrack|Sample` {name:'s5',control:'1'})")
                    .append("CREATE (s6:`vdp|VRTrack|Sample` {name:'s6'})")
                    .append("CREATE (g1)-[:has]->(stu1)")
                    .append("CREATE (g2)-[:has]->(stu2)")
                    .append("CREATE (g3)-[:has]->(stu3)")
                    .append("CREATE (g2)-[:has]->(stu4)")
                    .append("CREATE (g2)-[:has]->(stu5)")
                    .append("CREATE (g2)-[:has]->(stu6)")
                    .append("CREATE (stu2)-[:member]->(d2)")
                    .append("CREATE (stu3)-[:member]->(d1)")
                    .append("CREATE (stu3)-[:member]->(d3)")
                    .append("CREATE (d1)-[:sample]->(s1)")
                    .append("CREATE (d1)-[:sample]->(s2)")
                    .append("CREATE (d2)-[:sample]->(s3)")
                    .append("CREATE (d2)-[:sample]->(s4)")
                    .append("CREATE (d3)-[:sample]->(s5)")
                    .append("CREATE (d3)-[:sample]->(s6)")
                    .append("CREATE (stu3)-[:member]->(s1)")
                    .append("CREATE (stu3)-[:member]->(s2)")
                    .append("CREATE (stu2)-[:member]->(s3)")
                    .append("CREATE (stu2)-[:member]->(s4)")
                    .append("CREATE (stu3)-[:member]->(s5)")
                    .append("CREATE (stu3)-[:member]->(s6)")
                    .toString();
}
