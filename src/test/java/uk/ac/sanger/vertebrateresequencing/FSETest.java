package uk.ac.sanger.vertebrateresequencing;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.HashMap;
import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;

public class FSETest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);
    
    @Test
    public void shouldGetFiles() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_or_store_filesystem_paths/vdp/%2F/%2Fa%2Fb%2Flane1.cram").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("basename", "lane1.cram");
        prop.put("path", "/a/b/lane1.cram");
        prop.put("md5", "md5local");
        prop.put("neo4j_label", "FileSystemElement");
        expected.put("6", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_or_store_filesystem_paths/vdp/irods%3A%2F/%2Fa%2Fb%2Flane1.cram").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("basename", "lane1.cram");
        prop.put("path", "/a/b/lane1.cram");
        prop.put("md5", "md5irods");
        prop.put("neo4j_label", "FileSystemElement");
        expected.put("7", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_or_store_filesystem_paths/vdp/irods%3A%2F/%2Fa%2Fb%2Flane2.cram?only_get=1").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_or_store_filesystem_paths/vdp/irods%3A%2F/%2Fa%2Fb%2Flane2.cram").toString());
        actual = response.content();
        prop = (LinkedHashMap<String, Object>)actual.get("8");
        String uuid = (String)prop.remove("uuid");
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("basename", "lane2.cram");
        prop.put("path", "/a/b/lane2.cram");
        prop.put("neo4j_label", "FileSystemElement");
        expected.put("8", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_or_store_filesystem_paths/vdp/irods%3A%2F/%2Fa%2Fb%2Flane2.cram").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("basename", "lane2.cram");
        prop.put("path", "/a/b/lane2.cram");
        prop.put("uuid", uuid);
        prop.put("neo4j_label", "FileSystemElement");
        expected.put("8", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_or_store_filesystem_paths/vdp/irods%3A%2F/%2Fa%2Fb%2Flane2.cram%2F%2F%2F%2Fa%2Fc%2Fb%2Flane3.cram").toString());
        actual = response.content();
        prop = (LinkedHashMap<String, Object>)actual.get("11");
        prop.remove("uuid");
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("basename", "lane2.cram");
        prop.put("path", "/a/b/lane2.cram");
        prop.put("uuid", uuid);
        prop.put("neo4j_label", "FileSystemElement");
        expected.put("8", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("basename", "lane3.cram");
        prop.put("path", "/a/c/b/lane3.cram");
        prop.put("neo4j_label", "FileSystemElement");
        expected.put("11", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_or_store_filesystem_paths/vdp/%2F/%2F").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("basename", "/");
        prop.put("neo4j_label", "FileSystemElement");
        expected.put("0", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_or_store_filesystem_paths/vdp/ftp/%2F").toString());
        actual = response.content();
        prop = (LinkedHashMap<String, Object>)actual.get("12");
        prop.remove("uuid");
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("basename", "ftp");
        prop.put("neo4j_label", "FileSystemElement");
        expected.put("12", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/filesystemelement_to_path/6").toString());
        actual = response.content();
        LinkedHashMap<String, String> expectedStrs = new LinkedHashMap<String, String>();
        expectedStrs.put("path", "/a/b/lane1.cram");
        expectedStrs.put("root", "/");
        assertEquals(expectedStrs, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/filesystemelement_to_path/7").toString());
        actual = response.content();
        expectedStrs = new LinkedHashMap<String, String>();
        expectedStrs.put("path", "/a/b/lane1.cram");
        expectedStrs.put("root", "irods:/");
        assertEquals(expectedStrs, actual);
    }
    
    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (root:`vdp|VRPipe|FileSystemElement` {basename:'/'})")
                    .append("CREATE (irods:`vdp|VRPipe|FileSystemElement` {basename:'irods:/'})")
                    .append("CREATE (al:`vdp|VRPipe|FileSystemElement` {basename:'a',path:'/a'})")
                    .append("CREATE (bl:`vdp|VRPipe|FileSystemElement` {basename:'b',path:'/a/b'})")
                    .append("CREATE (ai:`vdp|VRPipe|FileSystemElement` {basename:'a',path:'/a'})")
                    .append("CREATE (bi:`vdp|VRPipe|FileSystemElement` {basename:'b',path:'/a/b'})")
                    .append("CREATE (craml:`vdp|VRPipe|FileSystemElement` {basename:'lane1.cram',path:'/a/b/lane1.cram',md5:'md5local'})")
                    .append("CREATE (crami:`vdp|VRPipe|FileSystemElement` {basename:'lane1.cram',path:'/a/b/lane1.cram',md5:'md5irods'})")
                    .append("CREATE (root)-[:contains]->(al)")
                    .append("CREATE (al)-[:contains]->(bl)")
                    .append("CREATE (bl)-[:contains]->(craml)")
                    .append("CREATE (irods)-[:contains]->(ai)")
                    .append("CREATE (ai)-[:contains]->(bi)")
                    .append("CREATE (bi)-[:contains]->(crami)")
                    .toString();
}