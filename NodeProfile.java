import java.util.ArrayList;


public class NodeProfile {
    boolean distinct;
    boolean where;
    ArrayList<String> tables;
    NodeGenerator where_clause;
    boolean order;
    String order_clause;
    ArrayList<String> arguments;

    public NodeProfile() {
        this.distinct = false;
        this.where = false;
        this.where_clause = null;
        this.order = false;
        this.order_clause = null;
        this.arguments = new ArrayList<>();
        this.tables = new ArrayList<>();
    }
}
