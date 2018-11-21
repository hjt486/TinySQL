import java.util.ArrayList;


public class TreeNode {
    boolean distinct;
    boolean where;
    ArrayList<String> t_names;
    ExTreeNode w_clause;
    boolean order;
    String o_clause;
    ArrayList<String> arg;

    public TreeNode() {
        this.distinct = false;
        this.where = false;
        this.w_clause = null;
        this.order = false;
        this.o_clause = null;
        this.arg = new ArrayList<>();
        this.t_names = new ArrayList<>();
    }
}
