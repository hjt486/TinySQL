import java.util.ArrayList;
/*
Jiatai Han & Qifan Li
11/2018

TinySQL Parser
*/

public class Parser {
    ArrayList<String> key_word;
    ArrayList<Attribute> arg;
    ArrayList<String> words;
    ArrayList<String> table_name;
    ArrayList<String> values;
    NodeProfile delete;
    NodeProfile select;

    public Parser() {
        key_word = new ArrayList<>();
        arg = new ArrayList<>();
        words = new ArrayList<>();
        table_name = new ArrayList<>();
        values = new ArrayList<>();
        delete = null;
        select = null;
    }

    private void reset(){
        key_word = new ArrayList<>();
        arg = new ArrayList<>();
        words = new ArrayList<>();
        table_name = new ArrayList<>();
        values = new ArrayList<>();
        delete = null;
        select = null;
    }

    public boolean syntax(String str){
        try {
            this.reset();
            str = str.trim();
            str = str.replace("//s{1,}"," ");
            String[] res = str.split(" ");

            if (res[0].equalsIgnoreCase("create")){
                key_word.add("create");
                if(!res[1].equalsIgnoreCase("table")) {
                    System.out.print("Illegal Input!");
                    return false;
                }
                table_name.add(res[2]);
                StringBuilder sb = new StringBuilder();
                for(int i = 3; i < res.length; i++){
                    sb.append(res[i] + " ");
                }
                String temp = sb.toString();

                if(temp.charAt(0) == '(' && temp.indexOf(")") > 0){
                    String sub = temp.substring(1,temp.indexOf(")"));
                    String[] args = sub.split(",");
                    for(int j = 0; j < args.length; j++){
                        args[j] = args[j].trim();
                        String [] field = args[j].split(" ");
                        if(field.length != 2){
                            System.out.print("Wrong argument!");
                            return false;
                        }
                        if(field[1].equalsIgnoreCase("str20")){
                            Attribute attribute = new Attribute(field[1],field[0]);
                            arg.add(attribute);
                        }else if(field[1].equalsIgnoreCase("int")){
                            Attribute attribute = new Attribute(field[1],field[0]);
                            arg.add(attribute);
                        }else{
                            System.out.println("Wrong input type for column \"”"+ field[0] +"\",");
                            System.out.println("Only INT or STR20 is allowed!");
                            return false;
                        }
                    }
                }

            } else if (res[0].equalsIgnoreCase("drop")){
                key_word.add("drop");
                if (!res[1].equalsIgnoreCase("table")){
                    System.out.print("Illegal Input!");
                    return false;
                }
                table_name.add(res[2]);
                if (res.length>3){
                    return false;
                }
            } else if (res[0].equalsIgnoreCase("insert")){
                key_word.add("insert");
               if (!res[1].equalsIgnoreCase("into")){
                   return false;
               }
               table_name.add(res[2]);
               int index = -1;
               int s_index = -1;
               for (int i = 3; i < res.length; i++){
                   if (res[i].equalsIgnoreCase("values")){
                       index = i;
                   }
                   if (res[i].equalsIgnoreCase("select")){
                       s_index = i;
                   }
               }

               if (index < 0 && s_index < 0){
                   System.out.print("No value!");
                   return false;
               }


                if (index > 0){
                    StringBuilder sb = new StringBuilder();
                    for(int i = 3; i < index; i++) {
                        sb.append(res[i] + " ");
                    }
                    String temp = sb.toString();
                    if (temp.charAt(0) == '(' && temp.indexOf(")") > 0){
                        String sub = temp.substring(1, temp.indexOf(")"));
                        String[] args = sub.split(",");
                        for (int j = 0; j < args.length; j++){
                            args[j] = args[j].trim();
                            String[] field = args[j].split(" ");
                            if (field.length != 1) {
                                System.out.print("Wrong argument!");
                                return false;
                            } else {
                                Attribute attribute = new Attribute(null, field[0]);
                                arg.add(attribute);
                            }
                        }
                    } else {
                        return false;
                    }

                    sb = new StringBuilder();
                    for (int i = index + 1; i < res.length; i++){
                        sb.append(res[i] + " ");
                    }

                    temp = sb.toString();
                    if (temp.charAt(0) == '(' && temp.indexOf(")") > 0){
                        String sub = temp.substring(1, temp.indexOf(")"));
                        String[] args = sub.split(",");
                        for (int j = 0; j < args.length; j++) {
                            args[j] = args[j].trim();
                            String[] field = args[j].split(" ");
                            if (field.length != 1) {
                                System.out.print("Wrong argument!");
                                return false;
                            } else {
                                values.add(field[0]);
                            }
                        }
                    } else {
                        return false;
                    }
                }else if(s_index > 0){
                    StringBuilder sb = new StringBuilder();
                    for(int i = 3; i < s_index; i++) {
                        sb.append(res[i] + " ");
                    }
                    String temp = sb.toString();
                    if(temp.charAt(0) == '(' && temp.indexOf(")") > 0) {
                        String sub = temp.substring(1, temp.indexOf(")"));
                        String[] args = sub.split(",");
                        for(int j = 0; j < args.length; j++) {
                            args[j] = args[j].trim();
                            String[] field = args[j].split(" ");
                            if (field.length != 1) {
                                System.out.print("Wrong argument!");
                                return false;
                            } else {
                                Attribute attribute = new Attribute(null, field[0]);
                                arg.add(attribute);
                            }
                        }
                    } else {
                        return false;
                    }

                    sb = new StringBuilder();
                    for(int i = s_index;i<res.length;i++){
                        sb.append(res[i]+" ");
                    }
                    return selectparse(sb.toString().split(" "));
                }

            }
            else if(res[0].equalsIgnoreCase("delete")){
                key_word.add("delete");
                delete = new NodeProfile();
                if(!res[1].equalsIgnoreCase("from")){
                    return false;
                }

                int index = -1;
                for(int i = 0; i < res.length; i++){
                    if(res[i].equalsIgnoreCase("where")){
                        index = i;
                        break;
                    }
                }

                if(index < 0){
                    index = res.length;
                } else {
                    delete.where = true;
                }

                StringBuilder sb1 = new StringBuilder();
                for(int i = 2; i < index;i++){
                    sb1.append(res[i]+" ");
                }
                String[] tables = sb1.toString().split(",");

                for(int i = 0; i < tables.length; i++){
                    delete.tables.add(tables[i].trim());
                }

                if(delete.where){
                    StringBuilder sb = new StringBuilder();
                    for(int i = index + 1; i < res.length; i++){
                        sb.append(res[i]+" ");
                    }
                    delete.where_clause = TreeGenerator.generate(sb.toString());
                }

            } else if(res[0].equalsIgnoreCase("select")){
                return selectparse(res);

            } else {
                return false;
            }

        }
        catch (Exception e) {
            return false;
        }
        return true;

    }

    private boolean selectparse(String[] res) {
        select = new NodeProfile();
        key_word.add("select");

        int from_index = -1;
        int where_index = -1;
        int order_index = -1;
        int distinct_index = -1;

        for(int i = 1; i < res.length; i++){
            if(res[i].equalsIgnoreCase("distinct")){
                distinct_index = i;
            }

            if(res[i].equalsIgnoreCase("from")){
                from_index = i;
            }

            if(res[i].equalsIgnoreCase("where")){
                where_index = i;
            }

            if(res[i].equalsIgnoreCase("order")){
                order_index = i;
            }
        }

        if(distinct_index == 1){
            select.distinct = true;
        }else if(distinct_index >= 0){
            return false;
        }

        if(where_index > 0 && order_index > 0 && where_index > order_index) {
            System.out.print("Illegal syntax! ORDER can not be in front of WHERE!");
            return false;
        }

        if(from_index < 0){
            System.out.print("Missing FROM!");
            return false;
        }
        StringBuilder sb = new StringBuilder();
        if(distinct_index > 0){
            for(int i = 2; i < from_index; i++){
                sb.append(res[i]+" ");
            }
            String[]arg_s = sb.toString().split(",");

            if(arg_s[0].trim().equalsIgnoreCase("*")){
                if(arg_s.length == 1) {
                    select.arguments.add("*");
                }else{
                    return false;
                }
            }else{
                for(int i = 0; i < arg_s.length; i++){
                    arg_s[i] = arg_s[i].trim();
                    select.arguments.add(arg_s[i]);
                }
            }
        } else {
            for(int i = 1; i < from_index; i++){
                sb.append(res[i]+" ");
            }
            String[]arg_s = sb.toString().split(",");

            if(arg_s[0].trim().equalsIgnoreCase("*")){
                if(arg_s.length == 1) {
                    select.arguments.add("*");
                } else {
                    return false;
                }
            } else {
                for(int i = 0; i < arg_s.length; i++){
                    arg_s[i] = arg_s[i].trim();
                    select.arguments.add(arg_s[i]);
                }
            }
        }

        sb = new StringBuilder();
        if(where_index > 0){
            select.where = true;
            for(int i = from_index + 1; i < where_index; i++){
                sb.append(res[i]+" ");
            }
            String[] tables = sb.toString().split(",");
            for(int i = 0; i < tables.length; i++){
                tables[i] = tables[i].trim();
                select.tables.add(tables[i]);
            }

            sb = new StringBuilder();
            if(order_index > 0) {
                select.order = true;
                for(int i = where_index + 1; i < order_index; i++){
                    sb.append(res[i]+" ");
                }
                select.where_clause = TreeGenerator.generate(sb.toString());

                if(!res[order_index+1].equalsIgnoreCase("by")){
                    System.out.print("Wrong ORDER BY!");
                    return false;
                }

                sb = new StringBuilder();
                for(int i = order_index+2; i < res.length; i++){
                    sb.append(res[i]+" ");
                }
                select.order_clause = sb.toString();
            } else {
                for(int i = where_index + 1; i < res.length; i++){
                    sb.append(res[i]+" ");
                }
                select.where_clause = TreeGenerator.generate(sb.toString());
            }
        } else {
            if(order_index > 0){
                if(!res[order_index + 1].equalsIgnoreCase("by")){
                    System.out.print("Wrong ORDER BY!");
                    return false;
                }

                select.order = true;
                for(int i = from_index + 1; i < order_index; i++){
                    sb.append(res[i]+" ");
                }
                String[] tables = sb.toString().split(",");
                for(int i = 0; i < tables.length; i++){
                    tables[i] = tables[i].trim();
                    select.tables.add(tables[i]);
                }

                sb = new StringBuilder();
                for(int i = order_index + 2; i < res.length; i++){
                    sb.append(res[i]+" ");
                }
                select.order_clause = sb.toString();
            } else {
                for(int i = from_index + 1; i < res.length; i++){
                    sb.append(res[i]+" ");
                }
                String[] tables = sb.toString().split(",");
                for(int i = 0; i < tables.length; i++){
                    tables[i] = tables[i].trim();
                    select.tables.add(tables[i]);
                }
            }
        }
        return true;
    }
}
