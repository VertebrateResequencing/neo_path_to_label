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
    
    @Test
    public void shouldGetSecondHierarchy() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_sequencing_hierarchy/vdp/10").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("name", "sec1");
        prop.put("neo4j_label", "Section");
        expected.put("10", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "bead1");
        prop.put("neo4j_label", "BeadChip");
        expected.put("11", prop);
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
    }
    
    @Test
    public void shouldGetThirdAndFourthHierarchy() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_sequencing_hierarchy/vdp/13").toString());
        HashMap actual = response.content();
        
        // only the lane changed from the first test
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("name", "lan2");
        prop.put("neo4j_label", "Lane");
        expected.put("13", prop);
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
        prop.put("name", "stu3");
        prop.put("neo4j_label", "Study");
        expected.put("20", prop);
        
        assertEquals(expected, actual);
        
        // this time is a new everything under study 1, with no donor
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_sequencing_hierarchy/vdp/15").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "lan3");
        prop.put("neo4j_label", "Lane");
        expected.put("15", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "lib2");
        prop.put("neo4j_label", "Library");
        expected.put("16", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "sam2");
        prop.put("neo4j_label", "Sample");
        expected.put("17", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "gen2");
        prop.put("neo4j_label", "Gender");
        expected.put("18", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "tax2");
        prop.put("neo4j_label", "Taxon");
        expected.put("19", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "stu1");
        prop.put("neo4j_label", "Study");
        expected.put("7", prop);
        
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
                    .append("CREATE (file2:`vdp|VRPipe|FileSystemElement`)")
                    .append("CREATE (section:`vdp|VRTrack|Section` {name:'sec1'})")
                    .append("CREATE (beadchip:`vdp|VRTrack|BeadChip` {name:'bead1'})")
                    .append("CREATE (file3:`vdp|VRPipe|FileSystemElement`)")
                    .append("CREATE (lane2:`vdp|VRTrack|Lane` {name:'lan2'})")
                    .append("CREATE (file4:`vdp|VRPipe|FileSystemElement`)")
                    .append("CREATE (lane3:`vdp|VRTrack|Lane` {name:'lan3'})")
                    .append("CREATE (lib2:`vdp|VRTrack|Library` {name:'lib2'})")
                    .append("CREATE (sample2:`vdp|VRTrack|Sample` {name:'sam2'})")
                    .append("CREATE (gender2:`vdp|VRTrack|Gender` {name:'gen2'})")
                    .append("CREATE (taxon2:`vdp|VRTrack|Taxon` {name:'tax2'})")
                    .append("CREATE (study3:`vdp|VRTrack|Study` {name:'stu3'})")
                    .append("CREATE (study1)-[:member]->(sample)")
                    .append("CREATE (study2)-[:member { preferred: '1' }]->(sample)")
                    .append("CREATE (study3)-[:member { preferred: '1' }]->(sample)")
                    .append("CREATE (study1)-[:member]->(donor)")
                    .append("CREATE (study2)-[:member]->(donor)")
                    .append("CREATE (study3)-[:member]->(donor)")
                    .append("CREATE (taxon)-[:member]->(sample)")
                    .append("CREATE (donor)-[:sample]->(sample)")
                    .append("CREATE (sample)-[:gender]->(gender)")
                    .append("CREATE (sample)-[:prepared]->(lib)")
                    .append("CREATE (lib)-[:sequenced]->(lane)")
                    .append("CREATE (lane)-[:aligned]->(file)")
                    .append("CREATE (section)-[:data]->(file2)")
                    .append("CREATE (sample)-[:placed]->(section)")
                    .append("CREATE (beadchip)-[:section]->(section)")
                    .append("CREATE (lane2)-[:aligned]->(file3)")
                    .append("CREATE (lib)-[:sequenced]->(lane2)")
                    .append("CREATE (study1)-[:member]->(sample2)")
                    .append("CREATE (taxon2)-[:member]->(sample2)")
                    .append("CREATE (sample2)-[:gender]->(gender2)")
                    .append("CREATE (sample2)-[:prepared]->(lib2)")
                    .append("CREATE (lib2)-[:sequenced]->(lane3)")
                    .append("CREATE (lane3)-[:aligned]->(file3)")
                    .append("CREATE (lane)-[:created_for]->(study2)")
                    .append("CREATE (lane2)-[:created_for]->(study3)")
                    .append("CREATE (section)-[:created_for]->(study2)")
                    .toString();
}
