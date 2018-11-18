import java.util.Stack;

/**
 * Created by lsr on 2016/11/30.
 */
public  class Builder {
    public static ExTreeNode generate(String input){
        Stack<String> operator = new Stack<>();
        Stack<ExTreeNode> opobejct= new Stack<>();
        input = input.replaceAll("\\({1}","( ");
        input = input.replaceAll("\\){1}"," )");
        input = input.replaceAll("\\s{1,}"," ");
        String[] words=input.split(" ");
        replace_key(words);

        for(int i=0;i<words.length;i++){
            words[i] = words[i].trim();
            char op = words[i].charAt(0);
            switch (op){
                case '+':
                case '-':
                case '*':
                case '/':
                case '&':
                case '|':
                case '=':
                case '>':
                case '<':
//                    operation handle;
//                    judge priority
                    int cur_priority = priority(words[i]);
                    while(!operator.isEmpty()&&cur_priority<priority(operator.peek())){
                        ExTreeNode right = opobejct.pop();
                        ExTreeNode left = opobejct.pop();
                        String op1 = operator.pop();
                        opobejct.push(new ExTreeNode(left,right,op1));
                    }
                    operator.push(words[i]);
                    break;
                case '(':
                    operator.push(words[i]);
                    break;
                case ')':
                    while(!operator.isEmpty()&& !operator.peek().equalsIgnoreCase("(")){
                        ExTreeNode right = opobejct.pop();
                        ExTreeNode left = opobejct.pop();
                        String op1 = operator.pop();
                        opobejct.push(new ExTreeNode(left,right,op1));
                    }
                    if(operator.peek()==null||!operator.peek().equalsIgnoreCase("(")){
                        System.out.print("wrong input");
                        return null;
                    }
                    operator.pop();
                    break;
                default:
                    opobejct.push(new ExTreeNode(words[i]));
            }
        }

        while(!operator.isEmpty()){
            ExTreeNode right = opobejct.pop();
            ExTreeNode left = opobejct.pop();
            String op1 = operator.pop();
            opobejct.push(new ExTreeNode(left,right,op1));
        }

        return opobejct.pop();
    }

    private static void replace_key(String[] words) {
        for(int i=0;i<words.length;i++){
            if(words[i].equalsIgnoreCase("and")){
                words[i] = "&";
                continue;
            }

            if(words[i].equalsIgnoreCase("or")){
                words[i] = "|";
                continue;
            }
        }
    }

    private static int priority(String word){
        char op = word.charAt(0);
        switch (op){
            case '|': return 0;
            case '&': return 1;
            case '>':
            case '<':
            case '=': return 2;
            case '+':
            case '-': return 3;
            case '*':
            case '/': return 4;
            default: return -1;
        }
    }
}
