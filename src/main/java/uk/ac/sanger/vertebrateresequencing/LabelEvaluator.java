package uk.ac.sanger.vertebrateresequencing;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

public class LabelEvaluator implements Evaluator {
    private Label label;
    private Long startNodeId;

    public LabelEvaluator(Label label, Long id) {
        this.label = label;
        this.startNodeId = id;
    }

    @Override
    public Evaluation evaluate(Path path) {
        Node lastNode = path.endNode();
        if (lastNode.getId() != startNodeId && lastNode.hasLabel(label)) {
            return Evaluation.INCLUDE_AND_PRUNE;
        }
        return Evaluation.EXCLUDE_AND_CONTINUE;
    }
}
