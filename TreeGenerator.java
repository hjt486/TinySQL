import java.util.Stack;

public  class TreeGenerator {
    public static NodeGenerator generator(String query){
        Stack<String> operator = new Stack<>();
        Stack<NodeGenerator> tree = new Stack<>();
        query = query.replaceAll("\\({1}","( ");
        query = query.replaceAll("\\){1}"," )");
        query = query.replaceAll("\\s{1,}"," ");
        String[] input_words = query.split(" ");
        word_to_op(input_words);

        for(int i = 0; i < input_words.length; i++){
            input_words[i] = input_words[i].trim();
            char op = input_words[i].charAt(0);
            if (op == '+'|| op == '-' || op == '*' || op == '/' || op == '&' || op == '|' || op == '=' || op == '>' || op == '<'){
                int current_precedence = precedence(input_words[i]);
                while(!operator.isEmpty() && current_precedence < precedence(operator.peek())){
                    NodeGenerator right = tree.pop();
                    NodeGenerator left = tree.pop();
                    String operator_temp = operator.pop();
                    tree.push(new NodeGenerator(left,right,operator_temp));
                }
                operator.push(input_words[i]);
            }
            else if (op == '(') {operator.push(input_words[i]);}
            else if (op == ')'){
                while(!operator.isEmpty() && !operator.peek().equalsIgnoreCase("(")){
                    NodeGenerator right = tree.pop();
                    NodeGenerator left = tree.pop();
                    String op1 = operator.pop();
                    tree.push(new NodeGenerator(left,right,op1));
                }
                if (operator.peek() == null || !operator.peek().equalsIgnoreCase("(")){
                    System.out.print("Input query is in illegal syntax!");
                    return null;
                }
                operator.pop();
            }
            else {tree.push(new NodeGenerator(input_words[i])); }
        }
        while(!operator.isEmpty()){
            NodeGenerator right = tree.pop();
            NodeGenerator left = tree.pop();
            String op1 = operator.pop();
            tree.push(new NodeGenerator(left,right,op1));
        }
        return tree.pop();
    }

    private static void word_to_op(String[] words) {
        for(int i = 0; i < words.length; i++){
            if (words[i].equalsIgnoreCase("and")) {
                words[i] = "&";
                continue;
            }
            if (words[i].equalsIgnoreCase("or")) {
                words[i] = "|";
            }
        }
    }

    private static int precedence(String word){
        char op = word.charAt(0);
        switch (op){
            case '|': return 0;
            case '&': return 1;
            case '>': return 2;
            case '<': return 2;
            case '=': return 2;
            case '+': return 3;
            case '-': return 3;
            case '*': return 4;
            case '/': return 4;
            default: return -1;
        }
    }
}
