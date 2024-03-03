package optimizer.rel;

import DB_Manager.Manager;

import java.util.List;

import org.apache.calcite.rel.RelNode;

public interface PRel extends RelNode {   
    public List <Object []> evaluate(Manager db_manager);
}