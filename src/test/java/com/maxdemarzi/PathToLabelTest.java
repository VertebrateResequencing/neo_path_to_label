package com.maxdemarzi;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.HashMap;
import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;

public class PathToLabelTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);
    
    @Test
    public void shouldFindFirstLabel() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/closest/One/to/0").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("name", "1");
        expected.put("1", prop);
        
        assertEquals(expected, actual);
    }
    
    @Test
    public void shouldFindSecondLabel() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/closest/Two/to/0").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("name", "2.1");
        expected.put("3", prop);
        
        assertEquals(expected, actual);
    }
    
    @Test
    public void shouldFindThreeLabel() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/closest/Three/to/0").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("name", "3.0");
        expected.put("4", prop);
        
        assertEquals(expected, actual);
    }
    
    @Test
    public void shouldFindFourLabel() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/closest/Four/to/0?direction=outgoing").toString());
        HashMap actual = response.content();
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);

        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/closest/Four/to/0?direction=incoming").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("name", "4.0");
        expected.put("5", prop);
        
        assertEquals(expected, actual);
    }
    
    @Test
    public void shouldFindFiveLabels() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/closest/Five/to/0?all=1").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("name", "a");
        expected.put("6", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "b");
        expected.put("7", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "c");
        expected.put("8", prop);
        
        assertEquals(expected, actual);
    }
    
    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (start:Start)")
                    .append("CREATE (one:One {name:'1'})")
                    .append("CREATE (two:Two {name:'2.0'})")
                    .append("CREATE (twotoo:Two {name:'2.1'})")
                    .append("CREATE (three:Three {name:'3.0'})")
                    .append("CREATE (four:Four {name:'4.0'})")
                    .append("CREATE (ay:Five {name:'a'})")
                    .append("CREATE (be:Five {name:'b'})")
                    .append("CREATE (ce:Five {name:'c'})")
                    .append("CREATE (de:Five {name:'d'})")
                    .append("CREATE (start)-[:CONNECTS]->(one)")
                    .append("CREATE (one)-[:CONNECTS]->(two)")
                    .append("CREATE (start)-[:CONNECTS]->(twotoo)")
                    .append("CREATE (one)-[:CONNECTS]->(three)")
                    .append("CREATE (two)-[:CONNECTS]->(three)")
                    .append("CREATE (twotoo)-[:CONNECTS]->(three)")
                    .append("CREATE (start)<-[:CONNECTS]-(four)")
                    .append("CREATE (three)-[:CONNECTS]->(ay)")
                    .append("CREATE (three)-[:CONNECTS]->(be)")
                    .append("CREATE (three)-[:CONNECTS]->(ce)")
                    .append("CREATE (ce)-[:CONNECTS]->(de)")
                    .toString();
}
