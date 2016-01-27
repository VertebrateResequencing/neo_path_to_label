# vrpipe_neo4j_plugin
Server Plugin required by VRPipe, handling certain queries faster than is possible with
cypher.

# Instructions

1. Build it:

        mvn clean package

2. Copy target/vrpipe_plugin-1.0-jar-with-dependencies.jar to the plugins/ directory of your Neo4j server.

3. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=uk.ac.sanger.vertebrateresequencing=/v1
        
4. Start Neo4j server.

5. Use the extension:
        
        :GET /v1/service/closest/{Label}/to/{node_id}
        :GET /v1/service/closest/{Label}/to/{node_id}?direction=incoming
        :GET /v1/service/closest/{Label}/to/{node_id}?direction=outgoing
        :GET /v1/service/closest/{Label}/to/{node_id}?depth=5
        :GET /v1/service/closest/{Label}/to/{node_id}?direction=outgoing&depth=5
	:GET /v1/service/closest/{Label}/to/{node_id}?all=1
	:GET /v1/service/closest/{Label}/to/{node_id}?all=1&properties=regex%40_%40reg%40_%40ex.%2A%40%40%40literal%40_%40foo%40_%40bar
	:GET /v1/service/get_sequencing_hierarchy/{db_label}/{lane_node_id}
        :GET /v1/service/donor_qc/{db_lable}/{user_name}/{donor_node_id}
	:GET /v1/service/get_node_with_extra_info/{db_lable}/{node_id}
	:GET /v1/service/vrtrack_nodes/{db_label}/{label}
	:GET /v1/service/vrtrack_file_qc/{db_label}/{enrypted_root}/{file_path}
	:GET /v1/service/vrtrack_alignment_files/{db_label}/Study%23id%231%2C2/.cram?parent_filter=Sample%23qc_failed%230
        
# COPYRIGHT & LICENSE

Copyright (c) 2015 Genome Research Limited.

This file is part of VRPipe.

VRPipe is free software: you can redistribute it and/or modify it under the
terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program. If not, see http://www.gnu.org/licenses/.

The usage of a range of years within a copyright statement contained within this
distribution should be interpreted as being equivalent to a list of years
including the first and last year specified and all consecutive years between
them. For example, a copyright statement that reads 'Copyright (c) 2005, 2007-
2009, 2011-2012' should be interpreted as being identical to a statement that
reads 'Copyright (c) 2005, 2007, 2008, 2009, 2011, 2012' and a copyright
statement that reads "Copyright (c) 2005-2012' should be interpreted as being
identical to a statement that reads 'Copyright (c) 2005, 2006, 2007, 2008, 2009,
2010, 2011, 2012'."


This repository was based on code by Max Demarzi. The original license follows:

The MIT License (MIT)

Copyright (c) 2015 Max De Marzi

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
