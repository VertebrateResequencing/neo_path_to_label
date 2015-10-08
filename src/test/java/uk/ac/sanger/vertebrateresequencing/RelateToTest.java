package uk.ac.sanger.vertebrateresequencing;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.HashMap;
import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;

public class RelateToTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);
    
    @Test
    public void shouldSetRelations() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/relate/0/onetotwo/1").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("neo4j_type", "onetotwo");
        prop.put("neo4j_startNode", "0");
        prop.put("neo4j_endNode", "1");
        expected.put("0", prop);
        
        assertEquals(expected, actual);
        
        // given non-existant node ids, it should silently and gracefully
        // return nothing
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/relate/10/preferred/1").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/relate/0/preferred/10").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);
        
        // test setting properties (we want to test that a new rel with higher
        // id is produced, so we can't do this in a separate test, since
        // apparently that resets everything)
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/relate/1/twotothree/2?properties=foo%40_%40bar%40%40%40car%40_%40goo").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("foo", "bar");
        prop.put("car", "goo");
        prop.put("neo4j_type", "twotothree");
        prop.put("neo4j_startNode", "1");
        prop.put("neo4j_endNode", "2");
        expected.put("1", prop);
        
        assertEquals(expected, actual);
        
        // repeating a relate doesn't create a new rel
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/relate/0/onetotwo/1").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("neo4j_type", "onetotwo");
        prop.put("neo4j_startNode", "0");
        prop.put("neo4j_endNode", "1");
        expected.put("0", prop);
        
        assertEquals(expected, actual);
        
        // selfish; first we need to create more rels to test it properly
        HTTP.GET(neo4j.httpURI().resolve("/v1/service/relate/3/selfishtest/4").toString());
        HTTP.GET(neo4j.httpURI().resolve("/v1/service/relate/3/selfishtest/5").toString());
        HTTP.GET(neo4j.httpURI().resolve("/v1/service/relate/4/selfishtest/6").toString());
        
        // I want to confirm what relationships there are, but since I can't
        // figure out how to call db methods in this context, I use closest
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/closest/Label/to/4?direction=outgoing&all=1").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "7");
        expected.put("6", prop);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/relate/3/selfishtest/6?selfish=1").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("neo4j_type", "selfishtest");
        prop.put("neo4j_startNode", "3");
        prop.put("neo4j_endNode", "6");
        expected.put("5", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/closest/Label/to/4?direction=outgoing&all=1").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/closest/Label/to/3?direction=outgoing&all=1").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "5");
        expected.put("4", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "6");
        expected.put("5", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "7");
        expected.put("6", prop);
        assertEquals(expected, actual);
        
        // replace
        HTTP.GET(neo4j.httpURI().resolve("/v1/service/relate/4/selfishtest/6").toString());
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/relate/3/selfishtest/6?replace=1").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("neo4j_type", "selfishtest");
        prop.put("neo4j_startNode", "3");
        prop.put("neo4j_endNode", "6");
        expected.put("5", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/closest/Label/to/4?direction=outgoing&all=1").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "7");
        expected.put("6", prop);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/closest/Label/to/3?direction=outgoing&all=1").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "7");
        expected.put("6", prop);
        assertEquals(expected, actual);
    }
    
    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (one:Label {name:'1'})")
                    .append("CREATE (two:Label {name:'2'})")
                    .append("CREATE (three:Label {name:'3'})")
                    .append("CREATE (four:Label {name:'4'})")
                    .append("CREATE (five:Label {name:'5'})")
                    .append("CREATE (six:Label {name:'6'})")
                    .append("CREATE (seven:Label {name:'7'})")
                    .toString();
}
