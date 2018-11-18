import java.util.ArrayList;

/**
 * Created by lsr on 2016/11/26.
 */
public class TreeNode {
    public boolean distinct;
    public boolean where;
    public ArrayList<String> t_names;
    public ExTreeNode w_clause;
    public boolean order;
    public String o_clause;
    public ArrayList<String> arg;

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
