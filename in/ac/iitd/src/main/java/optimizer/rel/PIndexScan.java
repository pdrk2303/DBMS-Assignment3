package optimizer.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import DB_Manager.Manager;

import java.util.EnumSet;
import java.util.List;
import java.util.ArrayList;

// Operator trigged when doing indexed scan
// Matches SFW queries with indexed columns in the WHERE clause
public class PIndexScan extends TableScan implements PRel {
    
        private final List<RexNode> projects;
        private final RelDataType rowType;
        private final RelOptTable table;
        private final RexNode filter;
    
        public PIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RexNode filter, List<RexNode> projects) {
            super(cluster, traitSet, table);
            this.table = table;
            this.rowType = deriveRowType();
            this.filter = filter;
            this.projects = projects;
        }
    
        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new PIndexScan(getCluster(), traitSet, table, filter, projects);
        }
    
        @Override
        public RelOptTable getTable() {
            return table;
        }

        @Override
        public String toString() {
            return "PIndexScan";
        }

        public String getTableName() {
            return table.getQualifiedName().get(1);
        }

        @Override
        public List<Object[]> evaluate(Manager db_manager) {
            String tableName = getTableName();
            System.out.println("Evaluating PIndexScan for table: " + tableName);

            /* Write your code here */

            return null;
        }
}