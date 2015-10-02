package uk.ac.sanger.vertebrateresequencing;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.HashMap;
import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;

public class HierarchyTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);
    
    @Test
    public void shouldGetFirstHierarchy() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_sequencing_hierarchy/vdp/1").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("name", "lan1");
        prop.put("neo4j_label", "Lane");
        expected.put("1", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "lib1");
        prop.put("neo4j_label", "Library");
        expected.put("2", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "sam1");
        prop.put("neo4j_label", "Sample");
        expected.put("3", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "gen1");
        prop.put("neo4j_label", "Gender");
        expected.put("4", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "tax1");
        prop.put("neo4j_label", "Taxon");
        expected.put("5", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "don1");
        prop.put("neo4j_label", "Donor");
        expected.put("6", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "stu2");
        prop.put("neo4j_label", "Study");
        expected.put("8", prop);
        
        assertEquals(expected, actual);
        
        // given a non-lane id, it should silently and gracefully return nothing
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_sequencing_hierarchy/vdp/0").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);
    }
    
    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (file:`vdp|VRPipe|FileSystemElement`)")
                    .append("CREATE (lane:`vdp|VRTrack|Lane` {name:'lan1'})")
                    .append("CREATE (lib:`vdp|VRTrack|Library` {name:'lib1'})")
                    .append("CREATE (sample:`vdp|VRTrack|Sample` {name:'sam1'})")
                    .append("CREATE (gender:`vdp|VRTrack|Gender` {name:'gen1'})")
                    .append("CREATE (taxon:`vdp|VRTrack|Taxon` {name:'tax1'})")
                    .append("CREATE (donor:`vdp|VRTrack|Donor` {name:'don1'})")
                    .append("CREATE (study1:`vdp|VRTrack|Study` {name:'stu1'})")
                    .append("CREATE (study2:`vdp|VRTrack|Study` {name:'stu2'})")
                    .append("CREATE (study1)-[:member]->(sample)")
                    .append("CREATE (study2)-[:member { preferred: '1' }]->(sample)")
                    .append("CREATE (study1)-[:member]->(donor)")
                    .append("CREATE (study2)-[:member]->(donor)")
                    .append("CREATE (taxon)-[:member]->(sample)")
                    .append("CREATE (donor)-[:sample]->(sample)")
                    .append("CREATE (sample)-[:gender]->(gender)")
                    .append("CREATE (sample)-[:prepared]->(lib)")
                    .append("CREATE (lib)-[:sequenced]->(lane)")
                    .append("CREATE (lane)-[:aligned]->(file)")
                    .toString();
}
