# neo_path_to_label
Traversal API finding the node with a specific label that is closest to another node.

# Instructions

1. Build it:

        mvn clean package

2. Copy target/path_to_label-1.0.jar to the plugins/ directory of your Neo4j server.


3. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=com.maxdemarzi=/v1
        
4. Start Neo4j server.

5. Use the extension:
        
        :GET /v1/service/closest/{Label}/to/{node_id}
        :GET /v1/service/closest/{Label}/to/{node_id}?direction=incoming
        :GET /v1/service/closest/{Label}/to/{node_id}?direction=outgoing
        :GET /v1/service/closest/{Label}/to/{node_id}?depth=5
        :GET /v1/service/closest/{Label}/to/{node_id}?direction=outgoing&depth=5
	:GET /v1/service/closest/{Label}/to/{node_id}?all=1
	:GET /v1/service/closest/{Label}/to/{node_id}?all=1&properties=regex%40_%40reg%40_%40ex.%2A%40%40%40literal%40_%40foo%40_%40bar
        
        
