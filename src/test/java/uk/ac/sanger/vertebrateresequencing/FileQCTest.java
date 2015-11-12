package uk.ac.sanger.vertebrateresequencing;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.HashMap;
import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;

public class FileQCTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);
    
    @Test
    public void shouldGetFileQC() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_file_qc/vdp/%2F/%2Fa%2Fb%2Flane1.bam").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("basename", "lane1.bam");
        prop.put("path", "/a/b/lane1.bam");
        prop.put("target", "1");
        prop.put("manual_qc", "1");
        prop.put("md5", "md52");
        prop.put("extra", "val");
        prop.put("neo4j_label", "FileSystemElement");
        expected.put("25", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("uuid", "g2");
        prop.put("date", "124");
        prop.put("neo4j_label", "Genotype");
        expected.put("12", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("uuid", "s1");
        prop.put("date", "125");
        prop.put("neo4j_label", "Bam_Stats");
        expected.put("13", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("uuid", "v2");
        prop.put("date", "127");
        prop.put("neo4j_label", "Verify_Bam_ID");
        expected.put("15", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("uuid", "h2");
        prop.put("neo4j_label", "Header_Mistakes");
        expected.put("17", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("uuid", "aqc1");
        prop.put("date", "128");
        prop.put("neo4j_label", "Auto_QC");
        expected.put("26", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "lan1");
        prop.put("neo4j_label", "Lane");
        expected.put("18", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "lib1");
        prop.put("neo4j_label", "Library");
        expected.put("19", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "sam1");
        prop.put("neo4j_label", "Sample");
        expected.put("20", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "gen1");
        prop.put("neo4j_label", "Gender");
        expected.put("21", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "tax1");
        prop.put("neo4j_label", "Taxon");
        expected.put("22", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "don1");
        prop.put("neo4j_label", "Donor");
        expected.put("23", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "stu1");
        prop.put("neo4j_label", "Study");
        expected.put("24", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_file_qc/vdp/%2F/%2Fa%2Fb%2Fsam1imported.csv").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("basename", "sam1imported.csv");
        prop.put("path", "/a/b/sam1imported.csv");
        prop.put("manual_qc", "1");
        prop.put("md5", "md5csv");
        prop.put("neo4j_label", "FileSystemElement");
        expected.put("29", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "sam1");
        prop.put("neo4j_label", "Sample");
        expected.put("20", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "gen1");
        prop.put("neo4j_label", "Gender");
        expected.put("21", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "tax1");
        prop.put("neo4j_label", "Taxon");
        expected.put("22", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "don1");
        prop.put("neo4j_label", "Donor");
        expected.put("23", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "stu2");
        prop.put("neo4j_label", "Study");
        expected.put("28", prop);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_file_qc/vdp/irods%3A%2F/%2Fa%2Fb%2Fsam1.csv").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        prop = new LinkedHashMap<String, Object>();
        prop.put("basename", "sam1.csv");
        prop.put("path", "/a/b/sam1.csv");
        prop.put("manual_qc", "1");
        prop.put("md5", "md5csv");
        prop.put("neo4j_label", "FileSystemElement");
        expected.put("27", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "sam1");
        prop.put("neo4j_label", "Sample");
        expected.put("20", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "gen1");
        prop.put("neo4j_label", "Gender");
        expected.put("21", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "tax1");
        prop.put("neo4j_label", "Taxon");
        expected.put("22", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "don1");
        prop.put("neo4j_label", "Donor");
        expected.put("23", prop);
        prop = new LinkedHashMap<String, Object>();
        prop.put("name", "stu2");
        prop.put("neo4j_label", "Study");
        expected.put("28", prop);
        
        assertEquals(expected, actual);
    }
    
    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (root:`vdp|VRPipe|FileSystemElement` {basename:'/'})")
                    .append("CREATE (irods:`vdp|VRPipe|FileSystemElement` {basename:'irods:/'})")
                    .append("CREATE (al:`vdp|VRPipe|FileSystemElement` {basename:'a',path:'/a'})")
                    .append("CREATE (bl:`vdp|VRPipe|FileSystemElement` {basename:'b',path:'/a/b'})")
                    .append("CREATE (ai:`vdp|VRPipe|FileSystemElement` {basename:'a',path:'/a'})")
                    .append("CREATE (bi:`vdp|VRPipe|FileSystemElement` {basename:'b',path:'/a/b'})")
                    .append("CREATE (cram1l:`vdp|VRPipe|FileSystemElement` {basename:'lane1.cram',path:'/a/b/lane1.cram'})")
                    .append("CREATE (cram1i:`vdp|VRPipe|FileSystemElement` {basename:'lane1.cram',path:'/a/b/lane1.cram',target:'1',manual_qc:'1',md5:'md51'})")
                    .append("CREATE (qcfile1:`vdp|VRPipe|FileSystemElement` {basename:'lane1.cram.geno',path:'/a/b/lane1.cram.geno'})")
                    .append("CREATE (qcfile2:`vdp|VRPipe|FileSystemElement` {basename:'lane1.cram.stats',path:'/a/b/lane1.cram.stats'})")
                    .append("CREATE (qcfile3:`vdp|VRPipe|FileSystemElement` {basename:'lane1.cram.verify',path:'/a/b/lane1.cram.verify'})")
                    .append("CREATE (geno1:`vdp|VRTrack|Genotype` {uuid:'g1',date:'123'})")
                    .append("CREATE (geno2:`vdp|VRTrack|Genotype` {uuid:'g2',date:'124'})")
                    .append("CREATE (stats:`vdp|VRTrack|Bam_Stats` {uuid:'s1',date:'125'})")
                    .append("CREATE (verify1:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v1',date:'126'})")
                    .append("CREATE (verify2:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v2',date:'127'})")
                    .append("CREATE (header1:`vdp|VRTrack|Header_Mistakes` {uuid:'h1'})")
                    .append("CREATE (header2:`vdp|VRTrack|Header_Mistakes` {uuid:'h2'})")
                    .append("CREATE (lane:`vdp|VRTrack|Lane` {name:'lan1'})")
                    .append("CREATE (lib:`vdp|VRTrack|Library` {name:'lib1'})")
                    .append("CREATE (sample:`vdp|VRTrack|Sample` {name:'sam1'})")
                    .append("CREATE (gender:`vdp|VRTrack|Gender` {name:'gen1'})")
                    .append("CREATE (taxon:`vdp|VRTrack|Taxon` {name:'tax1'})")
                    .append("CREATE (donor:`vdp|VRTrack|Donor` {name:'don1'})")
                    .append("CREATE (study:`vdp|VRTrack|Study` {name:'stu1'})")
                    .append("CREATE (cramimport:`vdp|VRPipe|FileSystemElement` {basename:'lane1.bam',path:'/a/b/lane1.bam',md5:'md52',extra:'val'})")
                    .append("CREATE (autoqc:`vdp|VRTrack|Auto_QC` {uuid:'aqc1',date:'128'})")
                    .append("CREATE (csv:`vdp|VRPipe|FileSystemElement` {basename:'sam1.csv',path:'/a/b/sam1.csv',manual_qc:'1',md5:'md5csv'})")
                    .append("CREATE (study2:`vdp|VRTrack|Study` {name:'stu2'})")
                    .append("CREATE (csvimport:`vdp|VRPipe|FileSystemElement` {basename:'sam1imported.csv',path:'/a/b/sam1imported.csv'})")
                    .append("CREATE (root)-[:contains]->(al)")
                    .append("CREATE (al)-[:contains]->(bl)")
                    .append("CREATE (bl)-[:contains]->(cram1l)")
                    .append("CREATE (irods)-[:contains]->(ai)")
                    .append("CREATE (ai)-[:contains]->(bi)")
                    .append("CREATE (bi)-[:contains]->(cram1i)")
                    .append("CREATE (cram1i)-[:qc_file]->(qcfile1)")
                    .append("CREATE (cram1i)-[:qc_file]->(qcfile2)")
                    .append("CREATE (cram1i)-[:qc_file]->(qcfile3)")
                    .append("CREATE (qcfile1)-[:genotype_data]->(geno1)")
                    .append("CREATE (qcfile1)-[:genotype_data]->(geno2)")
                    .append("CREATE (qcfile2)-[:summary_stats]->(stats)")
                    .append("CREATE (qcfile3)-[:verify_bam_id_data]->(verify1)")
                    .append("CREATE (qcfile3)-[:verify_bam_id_data]->(verify2)")
                    .append("CREATE (cram1i)-[:header_mistakes]->(header1)")
                    .append("CREATE (cram1i)-[:header_mistakes]->(header2)")
                    .append("CREATE (cram1i)-[:auto_qc_status]->(autoqc)")
                    .append("CREATE (study)-[:member]->(sample)")
                    .append("CREATE (study)-[:member]->(donor)")
                    .append("CREATE (taxon)-[:member]->(sample)")
                    .append("CREATE (donor)-[:sample]->(sample)")
                    .append("CREATE (sample)-[:gender]->(gender)")
                    .append("CREATE (sample)-[:prepared]->(lib)")
                    .append("CREATE (lib)-[:sequenced]->(lane)")
                    .append("CREATE (lane)-[:aligned]->(cram1i)")
                    .append("CREATE (cram1i)-[:imported]->(cramimport)")
                    .append("CREATE (bl)-[:contains]->(cramimport)")
                    .append("CREATE (sample)-[:processed]->(csv)")
                    .append("CREATE (bi)-[:contains]->(csv)")
                    .append("CREATE (csv)-[:created_for]->(study2)")
                    .append("CREATE (csv)-[:imported]->(csvimport)")
                    .append("CREATE (bl)-[:contains]->(csvimport)")
                    .toString();
}