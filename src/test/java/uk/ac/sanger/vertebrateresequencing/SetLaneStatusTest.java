package uk.ac.sanger.vertebrateresequencing;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class SetLaneStatusTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);
    
    @Test
    public void shouldSetLaneStatus() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_set_lane_qc_status/vdp/sb10/lan1/passed").toString());
        HashMap actual = response.content();
        LinkedHashMap<String, String> expected = new LinkedHashMap<String, String>();
        expected.put("success", "Lane lan1 qcgrind_qc_status set to passed");
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_set_lane_qc_status/vdp/sb10/lanx/passed").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, String>();
        expected.put("errors", "Lane lanx not found in graph db");
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_set_lane_qc_status/vdp/hacker/lan1/passed").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, String>();
        expected.put("errors", "User hacker does not administer any groups");
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_set_lane_qc_status/vdp/10sb/lan1/passed").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, String>();
        expected.put("errors", "User 10sb does not administer any groups that lane lan1 belongs to");
        assertEquals(expected, actual);
    }
    
    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (u1:`vdp|VRTrack|User` {username:'sb10'})")
                    .append("CREATE (g1:`vdp|VRTrack|Group` {name:'my project'})")
                    .append("CREATE (s1:`vdp|VRTrack|Study` {id:'1'})")
                    .append("CREATE (l1:`vdp|VRTrack|Lane` {unique:'lan1'})")
                    .append("CREATE (u2:`vdp|VRTrack|User` {username:'10sb'})")
                    .append("CREATE (u1)-[:administers]->(g1)")
                    .append("CREATE (g1)-[:has]->(s1)")
                    .append("CREATE (l1)-[:created_for]->(s1)")
                    .toString();
}
