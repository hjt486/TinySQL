import java.util.ArrayList;
import java.util.Stack;


/**
 * Created by lsr on 2016/11/26.
 */
public class ExTreeNode {
    ExTreeNode left;
    ExTreeNode right;
    String op;

    public ExTreeNode() {
        left = null;
        right = null;
        op = null;
    }

    public ExTreeNode(ExTreeNode left, ExTreeNode right, String op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public ExTreeNode(String op) {
        this.op = op;
        left = null;
        right = null;
    }

//    public ArrayList<ExTreeNode> hasSelection() {
//        if (left == null)
//            return null;
//        //check if the attribute has is table.attribute
//        char l = left.op.charAt(0);
//        char r = right.op.charAt(0);
//        boolean leftleaf  = Character.isLetter(l) || Character.isDigit(l);
//        boolean rightleaf = Character.isLetter(r) || Character.isDigit(r);
//
//        if (leftleaf && rightleaf) {
//            ArrayList<ExTreeNode> tree = new ArrayList<ExTreeNode>();
//            tree.add(this);
//            return tree;
//        }
//
//        ArrayList<ExTreeNode> lnode = left.hasSelection();
//        ArrayList<ExTreeNode> rnode = right.hasSelection();
//
//        if (lnode != null) {
//            if (rnode != null)
//                lnode.addAll(rnode);
//            return lnode;
//        }
//        else if(lnode == null && rnode != null) {
//            return rnode;
//        }
//
//        return null;
//    }

    public ArrayList<ExTreeNode> hasSelection(){
//        if(this == null){
//            return null;
//        }

//        if(left ==null && op !=null){
//            ArrayList<ExTreeNode> t = new ArrayList<>();
//            t.add(this);
//            return t;
//        }
        if(right ==null){
            return null;
        }

        if((Character.isDigit(this.left.op.charAt(0)) || Character.isLetter(this.left.op.charAt(0))) && (Character.isDigit(this.right.op.charAt(0)) || Character.isLetter(this.right.op.charAt(0)))){
            ArrayList<ExTreeNode> res = new ArrayList<>();
            res.add(this);
            return res;
        }

        ArrayList<ExTreeNode> left_ex = this.left.hasSelection();
        ArrayList<ExTreeNode> right_ex = this.right.hasSelection();

        if (left_ex!=null){
            if(right_ex!=null){
                left_ex.addAll(right_ex);
            }
            return left_ex;
        }else{
            if(right_ex!=null){
                return right_ex;
            }
            return null;
        }
    }


}