package uk.ac.sanger.vertebrateresequencing;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class DonorQCTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);
    
    @Test
    public void shouldGetFirstDonorQC() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/donor_qc/vdp/u2/11").toString());
        HashMap actual = response.content();
        
        LinkedHashMap<String, HashMap<String, Object>> expected = new LinkedHashMap<String, HashMap<String, Object>>();
        HashMap<String, Object> samples = new HashMap<String, Object>();
        HashMap<String, Object> sampleInfo1 = new HashMap<String, Object>();
        sampleInfo1.put("name", "s1");
        sampleInfo1.put("study_ids", "2,3");
        sampleInfo1.put("qc_status", "pending");
        sampleInfo1.put("qc_passed_fluidigm", false);
        sampleInfo1.put("qc_exclude_from_analysis", false);
        sampleInfo1.put("control", "1");
        sampleInfo1.put("public_name", "sp1");
        sampleInfo1.put("expected_gender", "M");
        sampleInfo1.put("actual_gender", "F");
        sampleInfo1.put("actual_gender_result_file", "/gender/result/file");
        sampleInfo1.put("discordance_genotyping", "[[\"4\",\"1050\",\"1.00\",\"s2\"],[\"5\",\"2050\",\"1.00\",\"s3\"]]");
        sampleInfo1.put("discordance_fluidigm", "[[\"4\",\"1070\",\"1.00\",\"s2\"],[\"5\",\"2070\",\"1.00\",\"s3\"]]");
        sampleInfo1.put("cnv_calls", "data2");
        sampleInfo1.put("loh_calls", "data2a");
        sampleInfo1.put("cnv_plot_1", "/path/to/cp1");
        sampleInfo1.put("cnv_plot_2", "/path/to/cp2");
        sampleInfo1.put("pluritest_summary", "data2b");
        samples.put("6", sampleInfo1);
        HashMap<String, Object> sampleInfo2 = new HashMap<String, Object>();
        sampleInfo2.put("name", "s2");
        sampleInfo2.put("study_ids", "2");
        sampleInfo2.put("qc_status", "failed");
        sampleInfo2.put("qc_passed_fluidigm", false);
        sampleInfo2.put("qc_exclude_from_analysis", false);
        sampleInfo2.put("control", "0");
        sampleInfo2.put("public_name", "sp2");
        sampleInfo2.put("qc_by", "u2");
        sampleInfo2.put("qc_time", "123");
        sampleInfo2.put("qc_failed_reason", "reason2");
        samples.put("7", sampleInfo2);
        HashMap<String, Object> sampleInfo3 = new HashMap<String, Object>();
        sampleInfo3.put("name", "s3");
        sampleInfo3.put("study_ids", "2");
        sampleInfo3.put("qc_status", "selected");
        sampleInfo3.put("qc_passed_fluidigm", true);
        sampleInfo3.put("qc_exclude_from_analysis", false);
        sampleInfo3.put("control", "0");
        sampleInfo3.put("public_name", "sp3");
        sampleInfo3.put("qc_by", "u2");
        sampleInfo3.put("qc_time", "124");
        sampleInfo3.put("expected_gender", "M");
        samples.put("8", sampleInfo3);
        HashMap<String, Object> sampleInfo4 = new HashMap<String, Object>();
        sampleInfo4.put("name", "s4");
        sampleInfo4.put("study_ids", "3");
        sampleInfo4.put("qc_status", "pending");
        sampleInfo4.put("qc_passed_fluidigm", true);
        sampleInfo4.put("qc_exclude_from_analysis", true);
        sampleInfo4.put("control", "0");
        sampleInfo4.put("public_name", "sp4");
        sampleInfo4.put("qc_by", "u2");
        sampleInfo4.put("qc_time", "125");
        samples.put("9", sampleInfo4);
        HashMap<String, Object> sampleInfo5 = new HashMap<String, Object>();
        sampleInfo5.put("name", "s5");
        sampleInfo5.put("study_ids", "3");
        sampleInfo5.put("qc_status", "pending");
        sampleInfo5.put("qc_passed_fluidigm", false);
        sampleInfo5.put("qc_exclude_from_analysis", false);
        sampleInfo5.put("control", "0");
        sampleInfo5.put("public_name", "sp5");
        samples.put("10", sampleInfo5);
        expected.put("samples", samples);
        HashMap<String, Object> adminDetails1 = new HashMap<String, Object>();
        adminDetails1.put("is_admin", "1");
        ArrayList<String> qcFailArray = new ArrayList<String>();
        qcFailArray.add("reason1");
        qcFailArray.add("reason2");
        adminDetails1.put("qc_fail_reasons", qcFailArray);
        expected.put("admin_details", adminDetails1);
        HashMap<String, Object> donorDetails = new HashMap<String, Object>();
        donorDetails.put("id", "d1");
        donorDetails.put("copy_number_by_chromosome_plot", "/path/to/cnbcp");
        donorDetails.put("pluritest_plot_clustering", "/path/to/plup1");
        donorDetails.put("pluritest_plot_novelty", "/path/to/plup2");
        expected.put("donor_details", donorDetails);
        
        assertEquals(expected, actual);
        
        // given a non-existing node id, it should silently and gracefully
        // return nothing
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/donor_qc/vdp/u2/111").toString());
        actual = response.content();
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        assertEquals(expected, actual);
        
        // trying to set qc status with a non-admin does nothing
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/donor_qc/vdp/u1/11?sample=s5&status=failed&reason=reason1&time=126").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        samples = new HashMap<String, Object>();
        samples.put("6", sampleInfo1);
        samples.put("7", sampleInfo2);
        samples.put("8", sampleInfo3);
        samples.put("9", sampleInfo4);
        samples.put("10", sampleInfo5);
        expected.put("samples", samples);
        HashMap<String, Object> adminDetails2 = new HashMap<String, Object>();
        adminDetails2.put("is_admin", "0");
        expected.put("admin_details", adminDetails2);
        expected.put("donor_details", donorDetails);
        
        assertEquals(expected, actual);
        
        // setting a pending sample to failed with admin works
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/donor_qc/vdp/u2/11?sample=s5&status=failed&reason=reason1&time=126").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        samples = new HashMap<String, Object>();
        samples.put("6", sampleInfo1);
        samples.put("7", sampleInfo2);
        samples.put("8", sampleInfo3);
        samples.put("9", sampleInfo4);
        HashMap<String, Object> sampleInfo5v2 = new HashMap<String, Object>();
        sampleInfo5v2.put("name", "s5");
        sampleInfo5v2.put("study_ids", "3");
        sampleInfo5v2.put("qc_status", "failed");
        sampleInfo5v2.put("qc_passed_fluidigm", false);
        sampleInfo5v2.put("qc_exclude_from_analysis", false);
        sampleInfo5v2.put("qc_failed_reason", "reason1");
        sampleInfo5v2.put("qc_time", "126");
        sampleInfo5v2.put("qc_by", "u2");
        sampleInfo5v2.put("control", "0");
        sampleInfo5v2.put("public_name", "sp5");
        samples.put("10", sampleInfo5v2);
        expected.put("samples", samples);
        expected.put("admin_details", adminDetails1);
        expected.put("donor_details", donorDetails);
        
        assertEquals(expected, actual);
        
        // switching status on the one we just set works
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/donor_qc/vdp/u2/11?sample=s5&status=selected&time=127").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        samples = new HashMap<String, Object>();
        samples.put("6", sampleInfo1);
        samples.put("7", sampleInfo2);
        samples.put("8", sampleInfo3);
        samples.put("9", sampleInfo4);
        sampleInfo5v2 = new HashMap<String, Object>();
        sampleInfo5v2.put("name", "s5");
        sampleInfo5v2.put("study_ids", "3");
        sampleInfo5v2.put("qc_status", "selected");
        sampleInfo5v2.put("qc_passed_fluidigm", false);
        sampleInfo5v2.put("qc_exclude_from_analysis", false);
        sampleInfo5v2.put("qc_time", "127");
        sampleInfo5v2.put("qc_by", "u2");
        sampleInfo5v2.put("control", "0");
        sampleInfo5v2.put("public_name", "sp5");
        samples.put("10", sampleInfo5v2);
        expected.put("samples", samples);
        expected.put("admin_details", adminDetails1);
        expected.put("donor_details", donorDetails);
        
        assertEquals(expected, actual);
        
        // overriding reason on an originally set one works
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/donor_qc/vdp/u2/11?sample=s2&status=failed&reason=reason1&time=128").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        samples = new HashMap<String, Object>();
        samples.put("6", sampleInfo1);
        HashMap<String, Object> sampleInfo2v2 = new HashMap<String, Object>();
        sampleInfo2v2.put("name", "s2");
        sampleInfo2v2.put("study_ids", "2");
        sampleInfo2v2.put("qc_status", "failed");
        sampleInfo2v2.put("qc_passed_fluidigm", false);
        sampleInfo2v2.put("qc_exclude_from_analysis", false);
        sampleInfo2v2.put("control", "0");
        sampleInfo2v2.put("public_name", "sp2");
        sampleInfo2v2.put("qc_by", "u2");
        sampleInfo2v2.put("qc_time", "128");
        sampleInfo2v2.put("qc_failed_reason", "reason1");
        samples.put("7", sampleInfo2v2);
        samples.put("8", sampleInfo3);
        samples.put("9", sampleInfo4);
        samples.put("10", sampleInfo5v2);
        expected.put("samples", samples);
        expected.put("admin_details", adminDetails1);
        expected.put("donor_details", donorDetails);
        
        assertEquals(expected, actual);
        
        // setting to defer works
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/donor_qc/vdp/u2/11?sample=s5&status=defer&time=129").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        samples = new HashMap<String, Object>();
        samples.put("6", sampleInfo1);
        samples.put("7", sampleInfo2v2);
        samples.put("8", sampleInfo3);
        samples.put("9", sampleInfo4);
        HashMap<String, Object> sampleInfo5v3 = new HashMap<String, Object>();
        sampleInfo5v3.put("name", "s5");
        sampleInfo5v3.put("study_ids", "3");
        sampleInfo5v3.put("qc_status", "defer");
        sampleInfo5v3.put("qc_passed_fluidigm", false);
        sampleInfo5v3.put("qc_exclude_from_analysis", false);
        sampleInfo5v3.put("qc_time", "129");
        sampleInfo5v3.put("qc_by", "u2");
        sampleInfo5v3.put("control", "0");
        sampleInfo5v3.put("public_name", "sp5");
        samples.put("10", sampleInfo5v3);
        expected.put("samples", samples);
        expected.put("admin_details", adminDetails1);
        expected.put("donor_details", donorDetails);
        
        assertEquals(expected, actual);
        
        // setting to excluded without passed does nothing
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/donor_qc/vdp/u2/11?sample=s5&status=set_excluded&time=130").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        samples = new HashMap<String, Object>();
        samples.put("6", sampleInfo1);
        samples.put("7", sampleInfo2v2);
        samples.put("8", sampleInfo3);
        samples.put("9", sampleInfo4);
        samples.put("10", sampleInfo5v3);
        expected.put("samples", samples);
        expected.put("admin_details", adminDetails1);
        expected.put("donor_details", donorDetails);
        
        assertEquals(expected, actual);
        
        // setting passed works
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/donor_qc/vdp/u2/11?sample=s5&status=set_passed&time=131").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        samples = new HashMap<String, Object>();
        samples.put("6", sampleInfo1);
        samples.put("7", sampleInfo2v2);
        samples.put("8", sampleInfo3);
        samples.put("9", sampleInfo4);
        sampleInfo5v3.put("qc_passed_fluidigm", true);
        samples.put("10", sampleInfo5v3);
        expected.put("samples", samples);
        expected.put("admin_details", adminDetails1);
        expected.put("donor_details", donorDetails);
        
        assertEquals(expected, actual);
        
        // setting to excluded works
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/donor_qc/vdp/u2/11?sample=s5&status=set_excluded&time=132").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        samples = new HashMap<String, Object>();
        samples.put("6", sampleInfo1);
        samples.put("7", sampleInfo2v2);
        samples.put("8", sampleInfo3);
        samples.put("9", sampleInfo4);
        sampleInfo5v3.put("qc_exclude_from_analysis", true);
        samples.put("10", sampleInfo5v3);
        expected.put("samples", samples);
        expected.put("admin_details", adminDetails1);
        expected.put("donor_details", donorDetails);
        
        assertEquals(expected, actual);
        
        // unsetting excluded works
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/donor_qc/vdp/u2/11?sample=s5&status=unset_excluded&time=133").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        samples = new HashMap<String, Object>();
        samples.put("6", sampleInfo1);
        samples.put("7", sampleInfo2v2);
        samples.put("8", sampleInfo3);
        samples.put("9", sampleInfo4);
        sampleInfo5v3.put("qc_exclude_from_analysis", false);
        samples.put("10", sampleInfo5v3);
        expected.put("samples", samples);
        expected.put("admin_details", adminDetails1);
        expected.put("donor_details", donorDetails);
        
        assertEquals(expected, actual);
        
        // supplying study ids limits what samples we get back from donor_qc
        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/donor_qc/vdp/u2/11?studies=3").toString());
        actual = response.content();
        
        expected = new LinkedHashMap<String, HashMap<String, Object>>();
        samples = new HashMap<String, Object>();
        samples.put("6", sampleInfo1);
        samples.put("7", sampleInfo2v2);
        samples.put("8", sampleInfo3);
        expected.put("samples", samples);
        expected.put("admin_details", adminDetails1);
        expected.put("donor_details", donorDetails);
        
        assertEquals(expected, actual);
    }
    
    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (g1:`vdp|VRTrack|Group` {name:'g1'})")
                    .append("CREATE (g2:`vdp|VRTrack|Group` {name:'g2', qc_fail_reasons:['reason1','reason2']})")
                    .append("CREATE (stu1:`vdp|VRTrack|Study` {id:'1'})")
                    .append("CREATE (stu2:`vdp|VRTrack|Study` {id:'2'})")
                    .append("CREATE (u1:`vdp|VRTrack|User` {username:'u1'})")
                    .append("CREATE (u2:`vdp|VRTrack|User` {username:'u2'})")
                    .append("CREATE (s1:`vdp|VRTrack|Sample` {name:'s1',control:'1',public_name:'sp1'})")
                    .append("CREATE (s2:`vdp|VRTrack|Sample` {name:'s2',control:'0',public_name:'sp2',qc_failed:'1'})")
                    .append("CREATE (s3:`vdp|VRTrack|Sample` {name:'s3',control:'0',public_name:'sp3',qc_passed:'1',qc_exclude_from_analysis:'0',qc_selected:'1'})")
                    .append("CREATE (s4:`vdp|VRTrack|Sample` {name:'s4',control:'0',public_name:'sp4',qc_passed:'1',qc_exclude_from_analysis:'1',qc_failed:'0',qc_selected:'0'})")
                    .append("CREATE (s5:`vdp|VRTrack|Sample` {name:'s5',control:'0',public_name:'sp5'})")
                    .append("CREATE (d1:`vdp|VRTrack|Donor` {id:'d1'})")
                    .append("CREATE (egender:`vdp|VRTrack|Gender` {gender:'M',source:'sequencescape'})")
                    .append("CREATE (agender:`vdp|VRTrack|Gender` {gender:'F',source:'fluidigm'})")
                    .append("CREATE (misc1:`vdp|VRTrack|FileSystemElement` {path:'misc1'})")
                    .append("CREATE (misc2:`vdp|VRTrack|FileSystemElement` {path:'misc2'})")
                    .append("CREATE (resultfile:`vdp|VRTrack|FileSystemElement` {path:'/gender/result/file'})")
                    .append("CREATE (stu3:`vdp|VRTrack|Study` {id:'3'})")
                    .append("CREATE (disc1:`vdp|VRTrack|Discordance` {type:'genotype',date:'1',cns:'{\"s1.s2\":[\"4\",\"1000\",\"1.00\",\"s2\"],\"s1.s3\":[\"5\",\"2000\",\"1.00\",\"s3\"]}'})")
                    .append("CREATE (disc2:`vdp|VRTrack|Discordance` {type:'genotype',date:'2',cns:'{\"s1.s2\":[\"4\",\"1050\",\"1.00\",\"s2\"],\"s1.s3\":[\"5\",\"2050\",\"1.00\",\"s3\"]}'})")
                    .append("CREATE (disc3:`vdp|VRTrack|Discordance` {type:'fluidigm',date:'3',cns:'{\"s1.o1\":[\"4\",\"100\",\"1.00\",\"o1\"],\"s1.s2\":[\"4\",\"1060\",\"1.00\",\"s2\"],\"s1.o2\":[\"4\",\"100\",\"1.00\",\"o2\"],\"s1.s3\":[\"5\",\"2060\",\"1.00\",\"s3\"]}'})")
                    .append("CREATE (disc4:`vdp|VRTrack|Discordance` {type:'fluidigm',date:'4',cns:'{\"s1.o1\":[\"4\",\"100\",\"1.00\",\"o1\"],\"s1.s2\":[\"4\",\"1070\",\"1.00\",\"s2\"],\"s1.o2\":[\"4\",\"100\",\"1.00\",\"o2\"],\"s1.s3\":[\"5\",\"2070\",\"1.00\",\"s3\"]}'})")
                    .append("CREATE (disc5:`vdp|VRTrack|Discordance` {type:'fluidigm',date:'4',cns:'{\"s5.o1\":[\"4\",\"100\",\"1.00\",\"o1\"],\"s5.s1\":[\"4\",\"1070\",\"1.00\",\"s1\"],\"s5.o2\":[\"4\",\"100\",\"1.00\",\"o2\"],\"s5.s2\":[\"5\",\"2070\",\"1.00\",\"s2\"]}'})")
                    .append("CREATE (cnvcall1:`vdp|VRTrack|CNVs` {date:'1',data:'data1'})")
                    .append("CREATE (cnvcall2:`vdp|VRTrack|CNVs` {date:'2',data:'data2'})")
                    .append("CREATE (lohcall1:`vdp|VRTrack|LOH` {date:'1',data:'data1a'})")
                    .append("CREATE (lohcall2:`vdp|VRTrack|LOH` {date:'2',data:'data2a'})")
                    .append("CREATE (cnbcp:`vdp|VRTrack|FileSystemElement` {path:'/path/to/cnbcp'})")
                    .append("CREATE (cp1:`vdp|VRTrack|FileSystemElement` {path:'/path/to/cp1',chr:'1'})")
                    .append("CREATE (cp2:`vdp|VRTrack|FileSystemElement` {path:'/path/to/cp2',chr:'2'})")
                    .append("CREATE (plucall1:`vdp|VRTrack|Pluritest` {date:'1',data:'data1b'})")
                    .append("CREATE (plucall2:`vdp|VRTrack|Pluritest` {date:'2',data:'data2b'})")
                    .append("CREATE (plup1:`vdp|VRTrack|FileSystemElement` {path:'/path/to/plup1',type:'clustering'})")
                    .append("CREATE (plup2:`vdp|VRTrack|FileSystemElement` {path:'/path/to/plup2',type:'novelty'})")
                    .append("CREATE (species:`vdp|VRTrack|Species` {id:'9606'})")
                    .append("CREATE (u2)-[:administers]->(g2)")
                    .append("CREATE (s2)-[:failed_by {time:'123',reason:'reason2'}]->(u2)")
                    .append("CREATE (s3)-[:selected_by {time:'124'}]->(u2)")
                    .append("CREATE (s4)-[:passed_by {time:'125'}]->(u2)")
                    .append("CREATE (d1)-[:sample]->(s1)")
                    .append("CREATE (d1)-[:sample]->(s2)")
                    .append("CREATE (d1)-[:sample]->(s3)")
                    .append("CREATE (d1)-[:sample]->(s4)")
                    .append("CREATE (d1)-[:sample]->(s5)")
                    .append("CREATE (g1)-[:has]->(stu1)")
                    .append("CREATE (g1)-[:has]->(stu2)")
                    .append("CREATE (g2)-[:has]->(stu1)")
                    .append("CREATE (g2)-[:has]->(stu2)")
                    .append("CREATE (stu2)-[:member]->(d1)")
                    .append("CREATE (stu3)-[:member]->(d1)")
                    .append("CREATE (s1)-[:gender]->(egender)")
                    .append("CREATE (s3)-[:gender]->(egender)")
                    .append("CREATE (s1)-[:processed]->(misc1)")
                    .append("CREATE (misc1)-[:imported]->(misc2)")
                    .append("CREATE (misc2)-[:converted]->(resultfile)")
                    .append("CREATE (resultfile)-[:gender]->(agender)")
                    .append("CREATE (stu2)-[:member]->(s1)")
                    .append("CREATE (stu3)-[:member]->(s1)")
                    .append("CREATE (stu2)-[:member]->(s2)")
                    .append("CREATE (stu2)-[:member]->(s3)")
                    .append("CREATE (stu3)-[:member]->(s4)")
                    .append("CREATE (stu3)-[:member]->(s5)")
                    .append("CREATE (species)-[:member]->(s1)")
                    .append("CREATE (species)-[:member]->(s1)")
                    .append("CREATE (species)-[:member]->(s2)")
                    .append("CREATE (species)-[:member]->(s3)")
                    .append("CREATE (species)-[:member]->(s4)")
                    .append("CREATE (species)-[:member]->(s5)")
                    .append("CREATE (s1)-[:discordance]->(disc1)")
                    .append("CREATE (s1)-[:discordance]->(disc2)")
                    .append("CREATE (s1)-[:discordance]->(disc3)")
                    .append("CREATE (s1)-[:discordance]->(disc4)")
                    .append("CREATE (s5)-[:discordance]->(disc5)")
                    .append("CREATE (s1)-[:cnv_calls]->(cnvcall1)")
                    .append("CREATE (s1)-[:cnv_calls]->(cnvcall2)")
                    .append("CREATE (s1)-[:loh_calls]->(lohcall1)")
                    .append("CREATE (s1)-[:loh_calls]->(lohcall2)")
                    .append("CREATE (s1)-[:copy_number_by_chromosome_plot]->(cnbcp)")
                    .append("CREATE (s1)-[:cnv_plot]->(cp1)")
                    .append("CREATE (s1)-[:cnv_plot]->(cp2)")
                    .append("CREATE (s1)-[:pluritest]->(plucall1)")
                    .append("CREATE (s1)-[:pluritest]->(plucall2)")
                    .append("CREATE (d1)-[:pluritest_plot]->(plup1)")
                    .append("CREATE (d1)-[:pluritest_plot]->(plup2)")
                    .toString();
}
