import java.util.*;

public class NodeGenerator {
    NodeGenerator left;
    NodeGenerator right;
    String operator;

    public NodeGenerator() {
        left = null;
        right = null;
        operator = null;
    }

    public NodeGenerator(String operator) {
        this.operator = operator;
        left = null;
        right = null;
    }

    public NodeGenerator(NodeGenerator left, NodeGenerator right, String operator) {
        this.left = left;
        this.right = right;
        this.operator = operator;
    }

    public ArrayList<NodeGenerator> hasSelection(){
        if(right == null){
            return null;
        }

        if((Character.isDigit(this.left.operator.charAt(0)) || Character.isLetter(this.left.operator.charAt(0))) && (Character.isDigit(this.right.operator.charAt(0)) || Character.isLetter(this.right.operator.charAt(0)))){
            ArrayList<NodeGenerator> result = new ArrayList<>();
            result.add(this);
            return result;
        }

        ArrayList<NodeGenerator> left = this.left.hasSelection();
        ArrayList<NodeGenerator> right = this.right.hasSelection();

        if (left != null){
            if(right != null){left.addAll(right);}
            return left;
        }else{
            if(right != null){return right;}
            return null;
        }
    }
}