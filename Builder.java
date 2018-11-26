import java.util.Stack;


public  class Builder {
    public static ExTreeNode generate(String input){
        Stack<String> operator = new Stack<>();
        Stack<ExTreeNode> tre = new Stack<>();
        input = input.replaceAll("\\({1}","( ");
        input = input.replaceAll("\\){1}"," )");
        input = input.replaceAll("\\s{1,}"," ");
        String[] words = input.split(" ");
        replace_key(words);

        for(int i = 0; i < words.length; i++){
            words[i] = words[i].trim();
            char op = words[i].charAt(0);
            if(op == '+'|| op == '-' || op == '*' || op == '/' || op == '&' || op == '|' || op == '=' || op == '>' || op == '<'){
                int cur_precedence = precedence(words[i]);
                while(!operator.isEmpty() && cur_precedence < precedence(operator.peek())){
                    ExTreeNode right = tre.pop();
                    ExTreeNode left = tre.pop();
                    String op1 = operator.pop();
                    tre.push(new ExTreeNode(left,right,op1));
                }
                operator.push(words[i]);
            }
            else if(op == '(') {
                operator.push(words[i]);
            }
            else if(op == ')'){
                while(!operator.isEmpty() && !operator.peek().equalsIgnoreCase("(")){
                    ExTreeNode right = tre.pop();
                    ExTreeNode left = tre.pop();
                    String op1 = operator.pop();
                    tre.push(new ExTreeNode(left,right,op1));
                }
                if(operator.peek() == null || !operator.peek().equalsIgnoreCase("(")){
                    System.out.print("wrong input");
                    return null;
                }
                operator.pop();
            }
            else{
                tre.push(new ExTreeNode(words[i]));
            }
        }
        while(!operator.isEmpty()){
            ExTreeNode right = tre.pop();
            ExTreeNode left = tre.pop();
            String op1 = operator.pop();
            tre.push(new ExTreeNode(left,right,op1));
        }
        return tre.pop();
    }

    private static void replace_key(String[] words) {
        for(int i = 0; i < words.length; i++){
            if(words[i].equalsIgnoreCase("and")){
                words[i] = "&";
                continue;
            }

            if(words[i].equalsIgnoreCase("or")){
                words[i] = "|";
                continue;
            }

            if(words[i].equalsIgnoreCase("not")){
                words[i] = "!";
                continue;
            }
        }
    }

    private static int precedence(String word){
        char op = word.charAt(0);
        switch (op){
            case '|': return 0;
            case '&': return 1;
            case '!': return 2;
            case '>': return 3;
            case '<': return 3;
            case '=': return 3;
            case '+': return 4;
            case '-': return 4;
            case '*': return 5;
            case '/': return 5;
            default: return -1;
        }
    }
}
