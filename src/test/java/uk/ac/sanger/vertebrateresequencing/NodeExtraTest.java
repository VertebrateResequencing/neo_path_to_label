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
        prop.put("qc_unresolved_fluidigm", "0");
        prop.put("qc_unresolved_genotyping", "0");
        prop.put("qc_unresolved", "1");
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
        prop.put("qc_defer", "1");
        prop.put("qc_passed", "1");
        expected.put("4", prop);
        
        assertEquals(expected, actual);
    }
    
    @Test
    public void shouldGetLaneWithExtra() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/get_node_with_extra_info/vdp/7").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        LinkedHashMap<String, Object> prop = new LinkedHashMap<String, Object>();
        prop.put("unique", "1");
        prop.put("qcgrind_qc_status", "passed");
        prop.put("npg_manual_qc", "1");
        prop.put("neo4j_label", "Lane");
        prop.put("study_id", "2");
        prop.put("study_name", "study two");
        prop.put("study_accession", "sacc2");
        prop.put("individual_name", "sp1");
        prop.put("sample_name", "s1");
        prop.put("sample_accession", "sacc1");
        prop.put("sample_supplier_name", "sn1");
        prop.put("gender", "M");
        prop.put("library_id", "1");
        prop.put("alignmentstats:result1", "1");
        prop.put("alignmentstats:result2", "2");
        prop.put("gtcheck", "unconfirmed (foo:0.88)");
        prop.put("gtcheckdata:matched_sample_name", "foo");
        prop.put("gtcheckdata:expected_sample_name", "foo");
        prop.put("gtcheckdata:pass", "0");
        prop.put("gtcheckdata:match_count", "14");
        prop.put("gtcheckdata:common_snp_count", "16");
        prop.put("gtcheckdata:otherresult", "5");
        prop.put("verifybamiddata:result1", "1");
        prop.put("verifybamiddata:result2", "2");
        prop.put("alignmentstats_plot:Coverage", "/seq/lan1.cram.plot.coverage");
        prop.put("alignmentstats_plot:Insert Size", "/seq/lan1.cram.plot.insert_size");
        prop.put("auto_qc:result1", "1");
        prop.put("auto_qc:result2", "2");
        expected.put("7", prop);
        
        assertEquals(expected, actual);
    }
    
    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (stu1:`vdp|VRTrack|Study` {id:'1'})")
                    .append("CREATE (stu2:`vdp|VRTrack|Study` {id:'2',name:'study two',accession:'sacc2'})")
                    .append("CREATE (s1:`vdp|VRTrack|Sample` {name:'s1',control:'1',public_name:'sp1',created_date:'123',qc_selected:'1',qc_passed:'1',qc_passed_genotyping:'1',accession:'sacc1',supplier_name:'sn1'})")
                    .append("CREATE (s2:`vdp|VRTrack|Sample` {name:'s2',control:'0',public_name:'sp2',created_date:'125',qc_freeze:'1',qc_passed:'1',qc_passed_genotyping:'1'})")
                    .append("CREATE (s3:`vdp|VRTrack|Sample` {name:'s3',control:'0',public_name:'sp3',created_date:'124',qc_defer:'1',qc_passed:'1'})")
                    .append("CREATE (d1:`vdp|VRTrack|Donor` {id:'d1'})")
                    .append("CREATE (lib1:`vdp|VRTrack|Library` {id:'1'})")
                    .append("CREATE (lan1:`vdp|VRTrack|Lane` {unique:'1',qcgrind_qc_status:'passed'})")
                    .append("CREATE (cram1:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan1.cram',basename:'lan1.cram',manual_qc:'1'})")
                    .append("CREATE (qcf1:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan1.cram.stats',basename:'lan1.cram.stats'})")
                    .append("CREATE (qcf2:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan1.cram.gtcheck',basename:'lan1.cram.gtcheck'})")
                    .append("CREATE (qcf3:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan1.cram.verify',basename:'lan1.cram.verify'})")
                    .append("CREATE (plot1:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan1.cram.plot.coverage',basename:'lan1.cram.plot.coverage',caption:'Coverage'})")
                    .append("CREATE (plot2:`vdp|VRPipe|FileSystemElement` {path:'/seq/lan1.cram.plot.insert_size',basename:'lan1.cram.plot.insert_size',caption:'Insert Size'})")
                    .append("CREATE (geno:`vdp|VRTrack|Genotype` {uuid:'g2',date:'124',matched_sample_name:'foo',expected_sample_name:'foo',pass:'0',match_count:'14',common_snp_count:'16',otherresult:'5'})")
                    .append("CREATE (stats:`vdp|VRTrack|Bam_Stats` {uuid:'s1',date:'125',result1:'1',result2:'2'})")
                    .append("CREATE (verify:`vdp|VRTrack|Verify_Bam_ID` {uuid:'v1',date:'126',result1:'1',result2:'2'})")
                    .append("CREATE (bam1:`vdp|VRPipe|FileSystemElement` {path:'/imported/lan1.bam',basename:'lan1.bam'})")
                    .append("CREATE (autoqc:`vdp|VRTrack|Auto_QC` {uuid:'aqc1',date:'128',result1:'1',result2:'2'})")
                    .append("CREATE (gender:`vdp|VRTrack|Gender` {gender:'M'})")
                    .append("CREATE (d1)-[:sample]->(s1)")
                    .append("CREATE (d1)-[:sample]->(s2)")
                    .append("CREATE (d1)-[:sample]->(s3)")
                    .append("CREATE (stu2)-[:member]->(d1)")
                    .append("CREATE (stu2)-[:member]->(s1)")
                    .append("CREATE (stu2)-[:member]->(s2)")
                    .append("CREATE (stu2)-[:member { preferred: '1' }]->(s3)")
                    .append("CREATE (stu1)-[:member]->(s3)")
                    .append("CREATE (s1)-[:gender]->(gender)")
                    .append("CREATE (s1)-[:prepared]->(lib1)")
                    .append("CREATE (lib1)-[:sequenced]->(lan1)")
                    .append("CREATE (lan1)-[:created_for]->(stu2)")
                    .append("CREATE (lan1)-[:aligned]->(cram1)")
                    .append("CREATE (cram1)-[:qc_file]->(qcf1)")
                    .append("CREATE (cram1)-[:qc_file]->(qcf2)")
                    .append("CREATE (cram1)-[:qc_file]->(qcf3)")
                    .append("CREATE (qcf1)-[:summary_stats]->(stats)")
                    .append("CREATE (qcf2)-[:genotype_data]->(geno)")
                    .append("CREATE (qcf3)-[:verify_bam_id_data]->(verify)")
                    .append("CREATE (qcf1)-[:bamstats_plot]->(plot1)")
                    .append("CREATE (qcf1)-[:bamstats_plot]->(plot2)")
                    .append("CREATE (cram1)-[:imported]->(bam1)")
                    .append("CREATE (bam1)-[:auto_qc_status]->(autoqc)")
                    .toString();
}
