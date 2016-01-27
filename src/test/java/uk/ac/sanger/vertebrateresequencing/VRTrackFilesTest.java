package uk.ac.sanger.vertebrateresequencing;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Arrays;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class VRTrackFilesTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);
    
    private void buildExpected (HashMap<String, HashMap<String, HashMap<String, String>>> expected, int lanIdStart, int lanIdEnd, ArrayList<Integer> skip, HashMap<String, String> qcProps) {
        int studyId = 1;
        int sampleId = 1;
        int libId = 1;
        int fseID = 0;
        int genId = 0;
        HashMap<Integer, Integer> skipHash = new HashMap<Integer, Integer>();
        for (Integer i: skip) {
            skipHash.put(i, Integer.valueOf(1));
        }
        for (int lanId = 1; lanId <= lanIdEnd; lanId++) {
            if (lanId == 7) {
                continue;
            }
            
            if (lanId == 9) {
                studyId++;
            }
            if ((lanId > 4 && lanId < 9) || lanId > 12) {
                genId = 2;
            }
            else {
                genId = 1;
            }
            
            if (lanId >= lanIdStart) {
                HashMap<String, HashMap<String, String>> fileResult = new LinkedHashMap<String, HashMap<String, String>>();
                
                if (! qcProps.isEmpty()) {
                    fileResult.put("qc_meta", qcProps);
                }
                
                HashMap<String, String> fileProps = new LinkedHashMap<String, String>();
                fileProps.put("basename", "lan" + lanId + ".cram");
                fileProps.put("md5", "lan" + lanId + "md5");
                
                if (lanId == 2 || lanId == 12) {
                    fileProps.put("manual_qc", "0");
                }
                else {
                    fileProps.put("manual_qc", "1");
                }
                
                fseID = lanId + 33;
                if (lanId > 1) {
                    fseID++;
                }
                if (lanId > 4 && lanId < 8) {
                    fseID++;
                }
                fileProps.put("node_id", fseID + "");
                fileResult.put("properties", fileProps);
                
                HashMap<String, String> hierarchyProps = new HashMap<String, String>();
                hierarchyProps.put("study_id", studyId + "");
                hierarchyProps.put("lane_name", "lan" + lanId);
                if (lanId == 1) {
                    hierarchyProps.put("lane_vrtrack_qc", "PASS");
                }
                else if (lanId == 2) {
                    hierarchyProps.put("lane_vrtrack_qc", "FAIL");
                }
                else if (lanId == 3) {
                    hierarchyProps.put("lane_vrtrack_qc", "MAYBE");
                }
                hierarchyProps.put("sample_name", "s" + sampleId);
                hierarchyProps.put("taxon_name", "tax1");
                hierarchyProps.put("library_name", "lib" + libId);
                hierarchyProps.put("gender_name", "gen" + genId);
                if (sampleId == 1) {
                    hierarchyProps.put("donor_id", "d1");
                }
                else if (sampleId == 2) {
                    hierarchyProps.put("sample_qc_failed", "0");
                }
                else if (sampleId == 3) {
                    hierarchyProps.put("sample_qc_failed", "1");
                }
                fileResult.put("hierarchy", hierarchyProps);
                
                if (! skipHash.containsKey(Integer.valueOf(lanId))) {
                    expected.put("/seq/lan" + lanId + ".cram", fileResult);
                }
            }
            
            if (lanId % 2 == 0) {
                libId++;
            }
            if (lanId % 4 == 0) {
                sampleId++;
            }
        }
    }
    
    @Test
    public void shouldGetVRTrackFiles() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_alignment_files/vdp/Study%23id%231%2C2/.cram").toString());
        HashMap actual = response.content();
        
        HashMap<String, HashMap<String, HashMap<String, String>>> expected = new LinkedHashMap<String, HashMap<String, HashMap<String, String>>>();
        ArrayList<Integer> skip = new ArrayList<Integer>();
        HashMap<String, String> qcProps = new HashMap<String, String>();
        buildExpected(expected, 1, 16, skip, qcProps);
        HashMap<String, HashMap<String, String>> statsResult = new HashMap<String, HashMap<String, String>>();
        HashMap<String, String> statProps = new HashMap<String, String>();
        statProps.put("passed", "15");
        statProps.put("failed parent filter", "0");
        statProps.put("failed qc filter", "0");
        statProps.put("failed no cram file", "0");
        statsResult.put("stats", statProps);
        expected.put("search", statsResult);
        
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_alignment_files/vdp/Study%23id%233/.cram").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, HashMap<String, String>>>();
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_alignment_files/vdp/Study%23id%232/.cram").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, HashMap<String, String>>>();
        buildExpected(expected, 9, 16, skip, qcProps);
        statProps.put("passed", "8");
        expected.put("search", statsResult);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_alignment_files/vdp/Study%23id%231/.cram").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, HashMap<String, String>>>();
        buildExpected(expected, 1, 8, skip, qcProps);
        statProps.put("passed", "7");
        expected.put("search", statsResult);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_alignment_files/vdp/Study%23id%231%2C2/.cram?parent_filter=Sample%23qc_failed%231").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, HashMap<String, String>>>();
        buildExpected(expected, 9, 12, skip, qcProps);
        statProps.put("passed", "4");
        statProps.put("failed parent filter", "11");
        expected.put("search", statsResult);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_alignment_files/vdp/Study%23id%231%2C2/.cram?parent_filter=Sample%23qc_failed%230").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, HashMap<String, String>>>();
        skip.addAll(Arrays.asList(9, 10, 11, 12));
        buildExpected(expected, 1, 16, skip, qcProps);
        skip.clear();
        statProps.put("passed", "11");
        statProps.put("failed parent filter", "4");
        expected.put("search", statsResult);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_alignment_files/vdp/Study%23id%231%2C2/.cram?parent_filter=Sample%23qc_failed%230%2CLane%23vrtrack_qc%23FAIL").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, HashMap<String, String>>>();
        buildExpected(expected, 2, 2, skip, qcProps);
        statProps.put("passed", "1");
        statProps.put("failed parent filter", "14");
        expected.put("search", statsResult);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_alignment_files/vdp/Study%23id%231%2C2/.cram?parent_filter=Sample%23qc_failed%230%2CLane%23vrtrack_qc%23PASS%2CLane%23vrtrack_qc%23MAYBE").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, HashMap<String, String>>>();
        skip.addAll(Arrays.asList(2));
        buildExpected(expected, 1, 3, skip, qcProps);
        skip.clear();
        statProps.put("passed", "2");
        statProps.put("failed parent filter", "13");
        expected.put("search", statsResult);
        assertEquals(expected, actual);
        
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_alignment_files/vdp/Study%23id%231%2C2/.cram?parent_filter=Sample%23qc_failed%230&qc_filter=file%23manual_qc%23%3D%231").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, HashMap<String, String>>>();
        skip.addAll(Arrays.asList(2, 9, 10, 11, 12));
        buildExpected(expected, 1, 16, skip, qcProps);
        skip.clear();
        statProps.put("passed", "10");
        statProps.put("failed parent filter", "3");
        statProps.put("failed qc filter", "2");
        expected.put("search", statsResult);
        assertEquals(expected, actual);
        
        // file#manual_qc#=#1,stats#sequences#>#10000,stats#reads QC failed#<#1000,genotype#pass#=#1,verifybamid#pass#=#1,header_mistakes#num_mistakes#=#0
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/vrtrack_alignment_files/vdp/Study%23id%231%2C2/.cram?parent_filter=Sample%23qc_failed%230&qc_filter=file%23manual_qc%23%3D%231%2Cstats%23sequences%23%3E%2310000%2Cstats%23reads%20QC%20failed%23%3C%231000%2Cgenotype%23pass%23%3D%231%2Cverifybamid%23pass%23%3D%231%2Cheader_mistakes%23num_mistakes%23%3D%230").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, HashMap<String, String>>>();
        skip.addAll(Arrays.asList(2, 3, 4, 6, 8, 9, 10, 11, 12, 13, 15));
        qcProps.put("stats_sequences", "20000");
        qcProps.put("genotype_pass", "1");
        qcProps.put("stats_reads QC failed", "500");
        qcProps.put("verifybamid_pass", "1");
        qcProps.put("header_mistakes_num_mistakes", "0");
        buildExpected(expected, 1, 16, skip, qcProps);
        skip.clear();
        statProps.put("passed", "4");
        statProps.put("failed parent filter", "3");
        statProps.put("failed qc filter", "8");
        expected.put("search", statsResult);
        assertEquals(expected, actual);
    }
    
    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (stu1:`vdp|VRTrack|Study` {id:'1'})")
                    .append("CREATE (stu2:`vdp|VRTrack|Study` {id:'2'})")
                    .append("CREATE (d1:`vdp|VRTrack|Donor` {id:'d1'})")
                    .append("CREATE (gen1:`vdp|VRTrack|Gender` {name:'gen1'})")
                    .append("CREATE (gen2:`vdp|VRTrack|Gender` {name:'gen2'})")
                    .append("CREATE (taxon:`vdp|VRTrack|Taxon` {name:'tax1'})")
                    .append("CREATE (s1:`vdp|VRTrack|Sample` {name:'s1'})")
                    .append("CREATE (s2:`vdp|VRTrack|Sample` {name:'s2',qc_failed:'0'})")
                    .append("CREATE (s3:`vdp|VRTrack|Sample` {name:'s3',qc_failed:'1'})")
                    .append("CREATE (s4:`vdp|VRTrack|Sample` {name:'s4'})")
                    .append("CREATE (lib1:`vdp|VRTrack|Library` {name:'lib1'})")
                    .append("CREATE (lib2:`vdp|VRTrack|Library` {name:'lib2'})")
                    .append("CREATE (lib3:`vdp|VRTrack|Library` {name:'lib3'})")
                    .append("CREATE (lib4:`vdp|VRTrack|Library` {name:'lib4'})")
                    .append("CREATE (lib5:`vdp|VRTrack|Library` {name:'lib5'})")
                    .append("CREATE (lib6:`vdp|VRTrack|Library` {name:'lib6'})")
                    .append("CREATE (lib7:`vdp|VRTrack|Library` {name:'lib7'})")
                    .append("CREATE (lib8:`vdp|VRTrack|Library` {name:'lib8'})")
                    .append("CREATE (lan1:`vdp|VRTrack|Lane` {name:'lan1',vrtrack_qc:'PASS'})")
                    .append("CREATE (lan2:`vdp|VRTrack|Lane` {name:'lan2',vrtrack_qc:'FAIL'})")
                    .append("CREATE (lan3:`vdp|VRTrack|Lane` {name:'lan3',vrtrack_qc:'MAYBE'})")
                    .append("CREATE (lan4:`vdp|VRTrack|Lane` {name:'lan4'})")
                    .append("CREATE (lan5:`vdp|VRTrack|Lane` {name:'lan5'})")
                    .append("CREATE (lan6:`vdp|VRTrack|Lane` {name:'lan6'})")
                    .append("CREATE (lan7:`vdp|VRTrack|Lane` {name:'lan7'})")
                    .append("CREATE (lan8:`vdp|VRTrack|Lane` {name:'lan8'})")
                    .append("CREATE (lan9:`vdp|VRTrack|Lane` {name:'lan9'})")
                    .append("CREATE (lan10:`vdp|VRTrack|Lane` {name:'lan10'})")
                    .append("CREATE (lan11:`vdp|VRTrack|Lane` {name:'lan11'})")
                    .append("CREATE (lan12:`vdp|VRTrack|Lane` {name:'lan12'})")
                    .append("CREATE (lan13:`vdp|VRTrack|Lane` {name:'lan13'})")
                    .append("CREATE (lan14:`vdp|VRTrack|Lane` {name:'lan14'})")
                    .append("CREATE (lan15:`vdp|VRTrack|Lane` {name:'lan15'})")
                    .append("CREATE (lan16:`vdp|VRTrack|Lane` {name:'lan16'})")
                    .append("CREATE (cram1:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan1.cram',basename:'lan1.cram',md5:'lan1md5',manual_qc:'1'})")
                    .append("CREATE (bam1:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan1.bam',basename:'lan1.bam',md5:'lan1bammd5',manual_qc:'1'})")
                    .append("CREATE (cram2:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan2.cram',basename:'lan2.cram',md5:'lan2md5',manual_qc:'0'})")
                    .append("CREATE (cram3:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan3.cram',basename:'lan3.cram',md5:'lan3md5',manual_qc:'1'})")
                    .append("CREATE (cram4:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan4.cram',basename:'lan4.cram',md5:'lan4md5',manual_qc:'1'})")
                    .append("CREATE (bam5:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan5.bam',basename:'lan5.bam',md5:'lan5bammd5',manual_qc:'1'})")
                    .append("CREATE (cram5:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan5.cram',basename:'lan5.cram',md5:'lan5md5',manual_qc:'1'})")
                    .append("CREATE (cram6:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan6.cram',basename:'lan6.cram',md5:'lan6md5',manual_qc:'1'})")
                    .append("CREATE (cram8:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan8.cram',basename:'lan8.cram',md5:'lan8md5',manual_qc:'1'})")
                    .append("CREATE (cram9:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan9.cram',basename:'lan9.cram',md5:'lan9md5',manual_qc:'1'})")
                    .append("CREATE (cram10:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan10.cram',basename:'lan10.cram',md5:'lan10md5',manual_qc:'1'})")
                    .append("CREATE (cram11:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan11.cram',basename:'lan11.cram',md5:'lan11md5',manual_qc:'1'})")
                    .append("CREATE (cram12:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan12.cram',basename:'lan12.cram',md5:'lan12md5',manual_qc:'0'})")
                    .append("CREATE (cram13:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan13.cram',basename:'lan13.cram',md5:'lan13md5',manual_qc:'1'})")
                    .append("CREATE (cram14:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan14.cram',basename:'lan14.cram',md5:'lan14md5',manual_qc:'1'})")
                    .append("CREATE (cram15:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan15.cram',basename:'lan15.cram',md5:'lan15md5',manual_qc:'1'})")
                    .append("CREATE (cram16:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan16.cram',basename:'lan16.cram',md5:'lan16md5',manual_qc:'1'})")
                    .append("CREATE (qcfile1:`vdp|VRPipe|FileSystemElement` {basename:'lane1.cram.geno',path:'/seq/lane1.cram.geno'})")
                    .append("CREATE (qcfile2:`vdp|VRPipe|FileSystemElement` {basename:'lane2.cram.geno',path:'/seq/lane2.cram.geno'})")
                    .append("CREATE (qcfile3:`vdp|VRPipe|FileSystemElement` {basename:'lane3.cram.geno',path:'/seq/lane3.cram.geno'})")
                    .append("CREATE (qcfile4:`vdp|VRPipe|FileSystemElement` {basename:'lane4.cram.geno',path:'/seq/lane4.cram.geno'})")
                    .append("CREATE (qcfile5:`vdp|VRPipe|FileSystemElement` {basename:'lane5.cram.geno',path:'/seq/lane5.cram.geno'})")
                    .append("CREATE (qcfile6:`vdp|VRPipe|FileSystemElement` {basename:'lane6.cram.geno',path:'/seq/lane6.cram.geno'})")
                    .append("CREATE (qcfile8:`vdp|VRPipe|FileSystemElement` {basename:'lane8.cram.geno',path:'/seq/lane8.cram.geno'})")
                    .append("CREATE (qcfile9:`vdp|VRPipe|FileSystemElement` {basename:'lane9.cram.geno',path:'/seq/lane9.cram.geno'})")
                    .append("CREATE (qcfile10:`vdp|VRPipe|FileSystemElement` {basename:'lane10.cram.geno',path:'/seq/lane10.cram.geno'})")
                    .append("CREATE (qcfile11:`vdp|VRPipe|FileSystemElement` {basename:'lane11.cram.geno',path:'/seq/lane11.cram.geno'})")
                    .append("CREATE (qcfile12:`vdp|VRPipe|FileSystemElement` {basename:'lane12.cram.geno',path:'/seq/lane12.cram.geno'})")
                    .append("CREATE (qcfile13:`vdp|VRPipe|FileSystemElement` {basename:'lane13.cram.geno',path:'/seq/lane13.cram.geno'})")
                    .append("CREATE (qcfile14:`vdp|VRPipe|FileSystemElement` {basename:'lane14.cram.geno',path:'/seq/lane14.cram.geno'})")
                    .append("CREATE (qcfile15:`vdp|VRPipe|FileSystemElement` {basename:'lane15.cram.geno',path:'/seq/lane15.cram.geno'})")
                    .append("CREATE (qcfile16:`vdp|VRPipe|FileSystemElement` {basename:'lane16.cram.geno',path:'/seq/lane16.cram.geno'})")
                    .append("CREATE (geno1:`vdp|VRTrack|Genotype` {uuid:'g1',date:'123',pass:'0'})")
                    .append("CREATE (geno1b:`vdp|VRTrack|Genotype` {uuid:'g1b',date:'124',pass:'1'})")
                    .append("CREATE (geno2:`vdp|VRTrack|Genotype` {uuid:'g2',date:'124',pass:'1'})")
                    .append("CREATE (geno3:`vdp|VRTrack|Genotype` {uuid:'g3',date:'124',pass:'0'})")
                    .append("CREATE (geno4:`vdp|VRTrack|Genotype` {uuid:'g4',date:'124',pass:'1'})")
                    .append("CREATE (geno5:`vdp|VRTrack|Genotype` {uuid:'g5',date:'124',pass:'1'})")
                    .append("CREATE (geno6:`vdp|VRTrack|Genotype` {uuid:'g6',date:'124',pass:'1'})")
                    .append("CREATE (geno8:`vdp|VRTrack|Genotype` {uuid:'g8',date:'124',pass:'1'})")
                    .append("CREATE (geno9:`vdp|VRTrack|Genotype` {uuid:'g9',date:'124',pass:'1'})")
                    .append("CREATE (geno10:`vdp|VRTrack|Genotype` {uuid:'g10',date:'124',pass:'1'})")
                    .append("CREATE (geno11:`vdp|VRTrack|Genotype` {uuid:'g11',date:'124',pass:'1'})")
                    .append("CREATE (geno12:`vdp|VRTrack|Genotype` {uuid:'g12',date:'124',pass:'1'})")
                    .append("CREATE (geno13:`vdp|VRTrack|Genotype` {uuid:'g13',date:'124',pass:'1'})")
                    .append("CREATE (geno14:`vdp|VRTrack|Genotype` {uuid:'g14',date:'124',pass:'1'})")
                    .append("CREATE (geno16:`vdp|VRTrack|Genotype` {uuid:'g16',date:'124',pass:'1'})")
                    .append("CREATE (qcfile17:`vdp|VRPipe|FileSystemElement` {basename:'lane1.cram.stats',path:'/seq/lane1.cram.stats'})")
                    .append("CREATE (qcfile18:`vdp|VRPipe|FileSystemElement` {basename:'lane2.cram.stats',path:'/seq/lane2.cram.stats'})")
                    .append("CREATE (qcfile19:`vdp|VRPipe|FileSystemElement` {basename:'lane3.cram.stats',path:'/seq/lane3.cram.stats'})")
                    .append("CREATE (qcfile20:`vdp|VRPipe|FileSystemElement` {basename:'lane4.cram.stats',path:'/seq/lane4.cram.stats'})")
                    .append("CREATE (qcfile21:`vdp|VRPipe|FileSystemElement` {basename:'lane5.cram.stats',path:'/seq/lane5.cram.stats'})")
                    .append("CREATE (qcfile22:`vdp|VRPipe|FileSystemElement` {basename:'lane6.cram.stats',path:'/seq/lane6.cram.stats'})")
                    .append("CREATE (qcfile23:`vdp|VRPipe|FileSystemElement` {basename:'lane8.cram.stats',path:'/seq/lane8.cram.stats'})")
                    .append("CREATE (qcfile24:`vdp|VRPipe|FileSystemElement` {basename:'lane9.cram.stats',path:'/seq/lane9.cram.stats'})")
                    .append("CREATE (qcfile25:`vdp|VRPipe|FileSystemElement` {basename:'lane10.cram.stats',path:'/seq/lane10.cram.stats'})")
                    .append("CREATE (qcfile26:`vdp|VRPipe|FileSystemElement` {basename:'lane11.cram.stats',path:'/seq/lane11.cram.stats'})")
                    .append("CREATE (qcfile27:`vdp|VRPipe|FileSystemElement` {basename:'lane12.cram.stats',path:'/seq/lane12.cram.stats'})")
                    .append("CREATE (qcfile28:`vdp|VRPipe|FileSystemElement` {basename:'lane13.cram.stats',path:'/seq/lane13.cram.stats'})")
                    .append("CREATE (qcfile29:`vdp|VRPipe|FileSystemElement` {basename:'lane14.cram.stats',path:'/seq/lane14.cram.stats'})")
                    .append("CREATE (qcfile30:`vdp|VRPipe|FileSystemElement` {basename:'lane15.cram.stats',path:'/seq/lane15.cram.stats'})")
                    .append("CREATE (qcfile31:`vdp|VRPipe|FileSystemElement` {basename:'lane16.cram.stats',path:'/seq/lane16.cram.stats'})")
                    .append("CREATE (stats1:`vdp|VRTrack|Bam_Stats` {uuid:'s1',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats2:`vdp|VRTrack|Bam_Stats` {uuid:'s2',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats3:`vdp|VRTrack|Bam_Stats` {uuid:'s3',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats4:`vdp|VRTrack|Bam_Stats` {uuid:'s4',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats4b:`vdp|VRTrack|Bam_Stats` {uuid:'s4b',date:'126',sequences:'10000',`reads QC failed`:'500'})")
                    .append("CREATE (stats5:`vdp|VRTrack|Bam_Stats` {uuid:'s5',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats6:`vdp|VRTrack|Bam_Stats` {uuid:'s6',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats8:`vdp|VRTrack|Bam_Stats` {uuid:'s8',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats9:`vdp|VRTrack|Bam_Stats` {uuid:'s9',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats10:`vdp|VRTrack|Bam_Stats` {uuid:'s10',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats11:`vdp|VRTrack|Bam_Stats` {uuid:'s11',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats12:`vdp|VRTrack|Bam_Stats` {uuid:'s12',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats13:`vdp|VRTrack|Bam_Stats` {uuid:'s13',date:'125',sequences:'20000',`reads QC failed`:'1000'})")
                    .append("CREATE (stats14:`vdp|VRTrack|Bam_Stats` {uuid:'s14',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats15:`vdp|VRTrack|Bam_Stats` {uuid:'s15',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (stats16:`vdp|VRTrack|Bam_Stats` {uuid:'s16',date:'125',sequences:'20000',`reads QC failed`:'500'})")
                    .append("CREATE (qcfile32:`vdp|VRPipe|FileSystemElement` {basename:'lane1.cram.vbi',path:'/seq/lane1.cram.vbi'})")
                    .append("CREATE (qcfile33:`vdp|VRPipe|FileSystemElement` {basename:'lane2.cram.vbi',path:'/seq/lane2.cram.vbi'})")
                    .append("CREATE (qcfile34:`vdp|VRPipe|FileSystemElement` {basename:'lane3.cram.vbi',path:'/seq/lane3.cram.vbi'})")
                    .append("CREATE (qcfile35:`vdp|VRPipe|FileSystemElement` {basename:'lane4.cram.vbi',path:'/seq/lane4.cram.vbi'})")
                    .append("CREATE (qcfile36:`vdp|VRPipe|FileSystemElement` {basename:'lane5.cram.vbi',path:'/seq/lane5.cram.vbi'})")
                    .append("CREATE (qcfile37:`vdp|VRPipe|FileSystemElement` {basename:'lane6.cram.vbi',path:'/seq/lane6.cram.vbi'})")
                    .append("CREATE (qcfile38:`vdp|VRPipe|FileSystemElement` {basename:'lane8.cram.vbi',path:'/seq/lane8.cram.vbi'})")
                    .append("CREATE (qcfile39:`vdp|VRPipe|FileSystemElement` {basename:'lane9.cram.vbi',path:'/seq/lane9.cram.vbi'})")
                    .append("CREATE (qcfile40:`vdp|VRPipe|FileSystemElement` {basename:'lane10.cram.vbi',path:'/seq/lane10.cram.vbi'})")
                    .append("CREATE (qcfile41:`vdp|VRPipe|FileSystemElement` {basename:'lane11.cram.vbi',path:'/seq/lane11.cram.vbi'})")
                    .append("CREATE (qcfile42:`vdp|VRPipe|FileSystemElement` {basename:'lane12.cram.vbi',path:'/seq/lane12.cram.vbi'})")
                    .append("CREATE (qcfile43:`vdp|VRPipe|FileSystemElement` {basename:'lane13.cram.vbi',path:'/seq/lane13.cram.vbi'})")
                    .append("CREATE (qcfile44:`vdp|VRPipe|FileSystemElement` {basename:'lane14.cram.vbi',path:'/seq/lane14.cram.vbi'})")
                    .append("CREATE (qcfile45:`vdp|VRPipe|FileSystemElement` {basename:'lane15.cram.vbi',path:'/seq/lane15.cram.vbi'})")
                    .append("CREATE (qcfile46:`vdp|VRPipe|FileSystemElement` {basename:'lane16.cram.vbi',path:'/seq/lane16.cram.vbi'})")
                    .append("CREATE (verify1:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v1',date:'127',pass:'1'})")
                    .append("CREATE (verify2:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v2',date:'127',pass:'1'})")
                    .append("CREATE (verify3:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v3',date:'127',pass:'1'})")
                    .append("CREATE (verify4:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v4',date:'127',pass:'1'})")
                    .append("CREATE (verify5:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v5',date:'127',pass:'1'})")
                    .append("CREATE (verify6:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v6',date:'127',pass:'0'})")
                    .append("CREATE (verify8:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v8',date:'127',pass:'1'})")
                    .append("CREATE (verify9:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v9',date:'127',pass:'1'})")
                    .append("CREATE (verify10:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v10',date:'127',pass:'1'})")
                    .append("CREATE (verify11:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v11',date:'127',pass:'1'})")
                    .append("CREATE (verify12:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v12',date:'127',pass:'1'})")
                    .append("CREATE (verify13:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v13',date:'127',pass:'1'})")
                    .append("CREATE (verify14:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v14',date:'127',pass:'1'})")
                    .append("CREATE (verify15:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v15',date:'127',pass:'1'})")
                    .append("CREATE (verify16:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v16',date:'127',pass:'1'})")
                    .append("CREATE (header1:`vdp|VRTrack|Header_Mistakes` {uuid:'h1',num_mistakes:'0'})")
                    .append("CREATE (header2:`vdp|VRTrack|Header_Mistakes` {uuid:'h2',num_mistakes:'0'})")
                    .append("CREATE (header3:`vdp|VRTrack|Header_Mistakes` {uuid:'h3',num_mistakes:'0'})")
                    .append("CREATE (header4:`vdp|VRTrack|Header_Mistakes` {uuid:'h4',num_mistakes:'0'})")
                    .append("CREATE (header5:`vdp|VRTrack|Header_Mistakes` {uuid:'h5',num_mistakes:'0'})")
                    .append("CREATE (header6:`vdp|VRTrack|Header_Mistakes` {uuid:'h6',num_mistakes:'0'})")
                    .append("CREATE (header8:`vdp|VRTrack|Header_Mistakes` {uuid:'h8',num_mistakes:'1'})")
                    .append("CREATE (header9:`vdp|VRTrack|Header_Mistakes` {uuid:'h9',num_mistakes:'0'})")
                    .append("CREATE (header10:`vdp|VRTrack|Header_Mistakes` {uuid:'h10',num_mistakes:'0'})")
                    .append("CREATE (header11:`vdp|VRTrack|Header_Mistakes` {uuid:'h11',num_mistakes:'0'})")
                    .append("CREATE (header12:`vdp|VRTrack|Header_Mistakes` {uuid:'h12',num_mistakes:'0'})")
                    .append("CREATE (header13:`vdp|VRTrack|Header_Mistakes` {uuid:'h13',num_mistakes:'0'})")
                    .append("CREATE (header14:`vdp|VRTrack|Header_Mistakes` {uuid:'h14',num_mistakes:'0'})")
                    .append("CREATE (header15:`vdp|VRTrack|Header_Mistakes` {uuid:'h15',num_mistakes:'0'})")
                    .append("CREATE (header16:`vdp|VRTrack|Header_Mistakes` {uuid:'h16',num_mistakes:'0'})")
                    .append("CREATE (stu1)-[:member]->(d1)")
                    .append("CREATE (d1)-[:sample]->(s1)")
                    .append("CREATE (stu1)-[:member]->(s1)")
                    .append("CREATE (stu1)-[:member]->(s2)")
                    .append("CREATE (stu2)-[:member]->(s4)")
                    .append("CREATE (taxon)-[:member]->(s1)")
                    .append("CREATE (taxon)-[:member]->(s2)")
                    .append("CREATE (taxon)-[:member]->(s3)")
                    .append("CREATE (taxon)-[:member]->(s4)")
                    .append("CREATE (s1)-[:gender]->(gen1)")
                    .append("CREATE (s2)-[:gender]->(gen2)")
                    .append("CREATE (s3)-[:gender]->(gen1)")
                    .append("CREATE (s4)-[:gender]->(gen2)")
                    .append("CREATE (s1)-[:prepared]->(lib1)")
                    .append("CREATE (s1)-[:prepared]->(lib2)")
                    .append("CREATE (s2)-[:prepared]->(lib3)")
                    .append("CREATE (s2)-[:prepared]->(lib4)")
                    .append("CREATE (s3)-[:prepared]->(lib5)")
                    .append("CREATE (s3)-[:prepared]->(lib6)")
                    .append("CREATE (s4)-[:prepared]->(lib7)")
                    .append("CREATE (s4)-[:prepared]->(lib8)")
                    .append("CREATE (lib1)-[:sequenced]->(lan1)")
                    .append("CREATE (lib1)-[:sequenced]->(lan2)")
                    .append("CREATE (lib2)-[:sequenced]->(lan3)")
                    .append("CREATE (lib2)-[:sequenced]->(lan4)")
                    .append("CREATE (lib3)-[:sequenced]->(lan5)")
                    .append("CREATE (lib3)-[:sequenced]->(lan6)")
                    .append("CREATE (lib4)-[:sequenced]->(lan7)")
                    .append("CREATE (lib4)-[:sequenced]->(lan8)")
                    .append("CREATE (lib5)-[:sequenced]->(lan9)")
                    .append("CREATE (lib5)-[:sequenced]->(lan10)")
                    .append("CREATE (lib6)-[:sequenced]->(lan11)")
                    .append("CREATE (lib6)-[:sequenced]->(lan12)")
                    .append("CREATE (lib7)-[:sequenced]->(lan13)")
                    .append("CREATE (lib7)-[:sequenced]->(lan14)")
                    .append("CREATE (lib8)-[:sequenced]->(lan15)")
                    .append("CREATE (lib8)-[:sequenced]->(lan16)")
                    .append("CREATE (lan1)-[:aligned]->(cram1)")
                    .append("CREATE (lan1)-[:aligned]->(bam1)")
                    .append("CREATE (lan2)-[:aligned]->(cram2)")
                    .append("CREATE (lan3)-[:aligned]->(cram3)")
                    .append("CREATE (lan4)-[:aligned]->(cram4)")
                    .append("CREATE (lan5)-[:aligned]->(bam5)")
                    .append("CREATE (lan5)-[:aligned]->(cram5)")
                    .append("CREATE (lan6)-[:aligned]->(cram6)")
                    .append("CREATE (lan8)-[:aligned]->(cram8)")
                    .append("CREATE (lan9)-[:aligned]->(cram9)")
                    .append("CREATE (lan10)-[:aligned]->(cram10)")
                    .append("CREATE (lan11)-[:aligned]->(cram11)")
                    .append("CREATE (lan12)-[:aligned]->(cram12)")
                    .append("CREATE (lan13)-[:aligned]->(cram13)")
                    .append("CREATE (lan14)-[:aligned]->(cram14)")
                    .append("CREATE (lan15)-[:aligned]->(cram15)")
                    .append("CREATE (lan16)-[:aligned]->(cram16)")
                    .append("CREATE (lan1)-[:created_for]->(stu1)")
                    .append("CREATE (lan2)-[:created_for]->(stu1)")
                    .append("CREATE (lan3)-[:created_for]->(stu1)")
                    .append("CREATE (lan4)-[:created_for]->(stu1)")
                    .append("CREATE (lan5)-[:created_for]->(stu1)")
                    .append("CREATE (lan6)-[:created_for]->(stu1)")
                    .append("CREATE (lan8)-[:created_for]->(stu1)")
                    .append("CREATE (lan9)-[:created_for]->(stu2)")
                    .append("CREATE (lan10)-[:created_for]->(stu2)")
                    .append("CREATE (lan11)-[:created_for]->(stu2)")
                    .append("CREATE (lan12)-[:created_for]->(stu2)")
                    .append("CREATE (lan13)-[:created_for]->(stu2)")
                    .append("CREATE (lan14)-[:created_for]->(stu2)")
                    .append("CREATE (lan15)-[:created_for]->(stu2)")
                    .append("CREATE (lan16)-[:created_for]->(stu2)")
                    .append("CREATE (cram1)-[:qc_file]->(qcfile1)")
                    .append("CREATE (cram2)-[:qc_file]->(qcfile2)")
                    .append("CREATE (cram3)-[:qc_file]->(qcfile3)")
                    .append("CREATE (cram4)-[:qc_file]->(qcfile4)")
                    .append("CREATE (cram5)-[:qc_file]->(qcfile5)")
                    .append("CREATE (cram6)-[:qc_file]->(qcfile6)")
                    .append("CREATE (cram8)-[:qc_file]->(qcfile8)")
                    .append("CREATE (cram9)-[:qc_file]->(qcfile9)")
                    .append("CREATE (cram10)-[:qc_file]->(qcfile10)")
                    .append("CREATE (cram11)-[:qc_file]->(qcfile11)")
                    .append("CREATE (cram12)-[:qc_file]->(qcfile12)")
                    .append("CREATE (cram13)-[:qc_file]->(qcfile13)")
                    .append("CREATE (cram14)-[:qc_file]->(qcfile14)")
                    .append("CREATE (cram15)-[:qc_file]->(qcfile15)")
                    .append("CREATE (cram16)-[:qc_file]->(qcfile16)")
                    .append("CREATE (qcfile1)-[:genotype_data]->(geno1)")
                    .append("CREATE (qcfile1)-[:genotype_data]->(geno1b)")
                    .append("CREATE (qcfile2)-[:genotype_data]->(geno2)")
                    .append("CREATE (qcfile3)-[:genotype_data]->(geno3)")
                    .append("CREATE (qcfile4)-[:genotype_data]->(geno4)")
                    .append("CREATE (qcfile5)-[:genotype_data]->(geno5)")
                    .append("CREATE (qcfile6)-[:genotype_data]->(geno6)")
                    .append("CREATE (qcfile8)-[:genotype_data]->(geno8)")
                    .append("CREATE (qcfile9)-[:genotype_data]->(geno9)")
                    .append("CREATE (qcfile10)-[:genotype_data]->(geno10)")
                    .append("CREATE (qcfile11)-[:genotype_data]->(geno11)")
                    .append("CREATE (qcfile12)-[:genotype_data]->(geno12)")
                    .append("CREATE (qcfile13)-[:genotype_data]->(geno13)")
                    .append("CREATE (qcfile14)-[:genotype_data]->(geno14)")
                    .append("CREATE (qcfile16)-[:genotype_data]->(geno16)")
                    .append("CREATE (cram1)-[:qc_file]->(qcfile17)")
                    .append("CREATE (cram2)-[:qc_file]->(qcfile18)")
                    .append("CREATE (cram3)-[:qc_file]->(qcfile19)")
                    .append("CREATE (cram4)-[:qc_file]->(qcfile20)")
                    .append("CREATE (cram5)-[:qc_file]->(qcfile21)")
                    .append("CREATE (cram6)-[:qc_file]->(qcfile22)")
                    .append("CREATE (cram8)-[:qc_file]->(qcfile23)")
                    .append("CREATE (cram9)-[:qc_file]->(qcfile24)")
                    .append("CREATE (cram10)-[:qc_file]->(qcfile25)")
                    .append("CREATE (cram11)-[:qc_file]->(qcfile26)")
                    .append("CREATE (cram12)-[:qc_file]->(qcfile27)")
                    .append("CREATE (cram13)-[:qc_file]->(qcfile28)")
                    .append("CREATE (cram14)-[:qc_file]->(qcfile29)")
                    .append("CREATE (cram15)-[:qc_file]->(qcfile30)")
                    .append("CREATE (cram16)-[:qc_file]->(qcfile31)")
                    .append("CREATE (qcfile17)-[:summary_stats]->(stats1)")
                    .append("CREATE (qcfile18)-[:summary_stats]->(stats2)")
                    .append("CREATE (qcfile19)-[:summary_stats]->(stats3)")
                    .append("CREATE (qcfile20)-[:summary_stats]->(stats4)")
                    .append("CREATE (qcfile20)-[:summary_stats]->(stats4b)")
                    .append("CREATE (qcfile21)-[:summary_stats]->(stats5)")
                    .append("CREATE (qcfile22)-[:summary_stats]->(stats6)")
                    .append("CREATE (qcfile23)-[:summary_stats]->(stats8)")
                    .append("CREATE (qcfile24)-[:summary_stats]->(stats9)")
                    .append("CREATE (qcfile25)-[:summary_stats]->(stats10)")
                    .append("CREATE (qcfile26)-[:summary_stats]->(stats11)")
                    .append("CREATE (qcfile27)-[:summary_stats]->(stats12)")
                    .append("CREATE (qcfile28)-[:summary_stats]->(stats13)")
                    .append("CREATE (qcfile29)-[:summary_stats]->(stats14)")
                    .append("CREATE (qcfile30)-[:summary_stats]->(stats15)")
                    .append("CREATE (qcfile31)-[:summary_stats]->(stats16)")
                    .append("CREATE (cram1)-[:qc_file]->(qcfile32)")
                    .append("CREATE (cram2)-[:qc_file]->(qcfile33)")
                    .append("CREATE (cram3)-[:qc_file]->(qcfile34)")
                    .append("CREATE (cram4)-[:qc_file]->(qcfile35)")
                    .append("CREATE (cram5)-[:qc_file]->(qcfile36)")
                    .append("CREATE (cram6)-[:qc_file]->(qcfile37)")
                    .append("CREATE (cram8)-[:qc_file]->(qcfile38)")
                    .append("CREATE (cram9)-[:qc_file]->(qcfile39)")
                    .append("CREATE (cram10)-[:qc_file]->(qcfile40)")
                    .append("CREATE (cram11)-[:qc_file]->(qcfile41)")
                    .append("CREATE (cram12)-[:qc_file]->(qcfile42)")
                    .append("CREATE (cram13)-[:qc_file]->(qcfile43)")
                    .append("CREATE (cram14)-[:qc_file]->(qcfile44)")
                    .append("CREATE (cram15)-[:qc_file]->(qcfile45)")
                    .append("CREATE (cram16)-[:qc_file]->(qcfile46)")
                    .append("CREATE (qcfile32)-[:verify_bam_id_data]->(verify1)")
                    .append("CREATE (qcfile33)-[:verify_bam_id_data]->(verify2)")
                    .append("CREATE (qcfile34)-[:verify_bam_id_data]->(verify3)")
                    .append("CREATE (qcfile35)-[:verify_bam_id_data]->(verify4)")
                    .append("CREATE (qcfile36)-[:verify_bam_id_data]->(verify5)")
                    .append("CREATE (qcfile37)-[:verify_bam_id_data]->(verify6)")
                    .append("CREATE (qcfile38)-[:verify_bam_id_data]->(verify8)")
                    .append("CREATE (qcfile39)-[:verify_bam_id_data]->(verify9)")
                    .append("CREATE (qcfile40)-[:verify_bam_id_data]->(verify10)")
                    .append("CREATE (qcfile41)-[:verify_bam_id_data]->(verify11)")
                    .append("CREATE (qcfile42)-[:verify_bam_id_data]->(verify12)")
                    .append("CREATE (qcfile43)-[:verify_bam_id_data]->(verify13)")
                    .append("CREATE (qcfile44)-[:verify_bam_id_data]->(verify14)")
                    .append("CREATE (qcfile45)-[:verify_bam_id_data]->(verify15)")
                    .append("CREATE (qcfile46)-[:verify_bam_id_data]->(verify16)")
                    .append("CREATE (cram1)-[:header_mistakes]->(header1)")
                    .append("CREATE (cram2)-[:header_mistakes]->(header2)")
                    .append("CREATE (cram3)-[:header_mistakes]->(header3)")
                    .append("CREATE (cram4)-[:header_mistakes]->(header4)")
                    .append("CREATE (cram5)-[:header_mistakes]->(header5)")
                    .append("CREATE (cram6)-[:header_mistakes]->(header6)")
                    .append("CREATE (cram8)-[:header_mistakes]->(header8)")
                    .append("CREATE (cram9)-[:header_mistakes]->(header9)")
                    .append("CREATE (cram10)-[:header_mistakes]->(header10)")
                    .append("CREATE (cram11)-[:header_mistakes]->(header11)")
                    .append("CREATE (cram12)-[:header_mistakes]->(header12)")
                    .append("CREATE (cram13)-[:header_mistakes]->(header13)")
                    .append("CREATE (cram14)-[:header_mistakes]->(header14)")
                    .append("CREATE (cram15)-[:header_mistakes]->(header15)")
                    .append("CREATE (cram16)-[:header_mistakes]->(header16)")
                    .toString();
}
