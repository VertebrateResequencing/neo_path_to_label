package uk.ac.sanger.vertebrateresequencing;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class NodeExtraTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);
    
    @Test
    public void shouldGetDonorWithExtra() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_node_with_extra_info/vdp/5").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("id", "d1");
        prop.put("neo4j_label", "Donor");
        prop.put("example_sample", "sp1");
        prop.put("last_sample_added_date", "125");
        expected.put("5", prop);
        
        assertEquals(expected, actual);
    }
    
    @Test
    public void shouldGetSampleWithExtra() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_node_with_extra_info/vdp/4").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("name", "s3");
        prop.put("public_name", "sp3");
        prop.put("control", "0");
        prop.put("created_date", "124");
        prop.put("neo4j_label", "Sample");
        prop.put("donor_node_id", "5");
        prop.put("donor_id", "d1");
        prop.put("study_node_id", "1");
        prop.put("study_id", "2");
        expected.put("4", prop);
        
        assertEquals(expected, actual);
    }
    
    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (stu1:`vdp|VRTrack|Study` {id:'1'})")
                    .append("CREATE (stu2:`vdp|VRTrack|Study` {id:'2'})")
                    .append("CREATE (s1:`vdp|VRTrack|Sample` {name:'s1',control:'1',public_name:'sp1',created_date:'123'})")
                    .append("CREATE (s2:`vdp|VRTrack|Sample` {name:'s2',control:'0',public_name:'sp2',created_date:'125'})")
                    .append("CREATE (s3:`vdp|VRTrack|Sample` {name:'s3',control:'0',public_name:'sp3',created_date:'124'})")
                    .append("CREATE (d1:`vdp|VRTrack|Donor` {id:'d1'})")
                    .append("CREATE (d1)-[:sample]->(s1)")
                    .append("CREATE (d1)-[:sample]->(s2)")
                    .append("CREATE (d1)-[:sample]->(s3)")
                    .append("CREATE (stu2)-[:member]->(d1)")
                    .append("CREATE (stu2)-[:member]->(s1)")
                    .append("CREATE (stu2)-[:member]->(s2)")
                    .append("CREATE (stu2)-[:member { preferred: '1' }]->(s3)")
                    .append("CREATE (stu1)-[:member]->(s3)")
                    .toString();
}
