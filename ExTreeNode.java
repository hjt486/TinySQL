import java.util.*;

public class ExTreeNode {
    ExTreeNode left;
    ExTreeNode right;
    String op;

    public ExTreeNode() {
        left = null;
        right = null;
        op = null;
    }

    public ExTreeNode(String op) {
        this.op = op;
        left = null;
        right = null;
    }

    public ExTreeNode(ExTreeNode left, ExTreeNode right, String op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public ArrayList<ExTreeNode> hasSelection(){
        if(right == null){
            return null;
        }

        if((Character.isDigit(this.left.op.charAt(0)) || Character.isLetter(this.left.op.charAt(0))) && (Character.isDigit(this.right.op.charAt(0)) || Character.isLetter(this.right.op.charAt(0)))){
            ArrayList<ExTreeNode> res = new ArrayList<>();
            res.add(this);
            return res;
        }

        ArrayList<ExTreeNode> left_ex = this.left.hasSelection();
        ArrayList<ExTreeNode> right_ex = this.right.hasSelection();

        if (left_ex != null){
            if(right_ex != null){
                left_ex.addAll(right_ex);
            }
            return left_ex;
        }else{
            if(right_ex != null){
                return right_ex;
            }
            return null;
        }
    }


}