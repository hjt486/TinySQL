import java.util.*;

public class NodeGenerator {
    NodeGenerator left;
    NodeGenerator right;
    String op;

    public NodeGenerator() {
        left = null;
        right = null;
        op = null;
    }

    public NodeGenerator(String op) {
        this.op = op;
        left = null;
        right = null;
    }

    public NodeGenerator(NodeGenerator left, NodeGenerator right, String op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public ArrayList<NodeGenerator> hasSelection(){
        if(right == null){
            return null;
        }

        if((Character.isDigit(this.left.op.charAt(0)) || Character.isLetter(this.left.op.charAt(0))) && (Character.isDigit(this.right.op.charAt(0)) || Character.isLetter(this.right.op.charAt(0)))){
            ArrayList<NodeGenerator> res = new ArrayList<>();
            res.add(this);
            return res;
        }

        ArrayList<NodeGenerator> left_ex = this.left.hasSelection();
        ArrayList<NodeGenerator> right_ex = this.right.hasSelection();

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