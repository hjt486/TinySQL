
import java.util.ArrayList;


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

            if(res[0].equalsIgnoreCase("create")){
                key_word.add("create");
                if(!res[1].equalsIgnoreCase("table")) {
                    System.out.print("Illegal Input");
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
                            System.out.print("Wrong Arg Format");
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

            }else if(res[0].equalsIgnoreCase("drop")){
                key_word.add("drop");
                if(!res[1].equalsIgnoreCase("table")){
                    System.out.print("Illegal Input");
                    return false;
                }
                table_name.add(res[2]);
                if(res.length>3){
                    return false;
                }
            }
            else if(res[0].equalsIgnoreCase("insert")){
                key_word.add("insert");
               if(!res[1].equalsIgnoreCase("into")){
                   return false;
               }
               table_name.add(res[2]);
               int index = -1;
               int s_index = -1;
               for(int i = 3; i < res.length; i++){
                   if(res[i].equalsIgnoreCase("values")){
                       index = i;
                   }
                   if(res[i].equalsIgnoreCase("select")){
                       s_index = i;
                   }
               }

               if(index < 0 && s_index < 0){
                   System.out.print("no values");
                   return false;
               }


                if(index > 0) {
                    StringBuilder sb = new StringBuilder();
                    for(int i = 3; i < index; i++) {
                        sb.append(res[i] + " ");
                    }
                    String temp = sb.toString();
                    if(temp.charAt(0) == '(' && temp.indexOf(")") > 0) {
                        String sub = temp.substring(1, temp.indexOf(")"));
                        String[] args = sub.split(",");
                        for (int j = 0; j < args.length; j++) {
                            args[j] = args[j].trim();
                            String[] field = args[j].split(" ");
                            if (field.length != 1) {
                                System.out.print("Wrong Arg Format");
                                return false;
                            } else {
                                Attribute attribute = new Attribute(null, field[0]);
                                arg.add(attribute);
                            }
                        }
                    }else{
                        return false;
                    }

                    sb = new StringBuilder();
                    for (int i = index + 1; i < res.length; i++) {
                        sb.append(res[i] + " ");
                    }

                    temp = sb.toString();
                    if(temp.charAt(0) == '(' && temp.indexOf(")") > 0) {
                        String sub = temp.substring(1, temp.indexOf(")"));
                        String[] args = sub.split(",");
                        for (int j = 0; j < args.length; j++) {
                            args[j] = args[j].trim();
                            String[] field = args[j].split(" ");
                            if (field.length != 1) {
                                System.out.print("Wrong Arg Format");
                                return false;
                            } else {
                                values.add(field[0]);
                            }
                        }
                    }else{
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
                            if(field.length != 1) {
                                System.out.print("Wrong Arg Format");
                                return false;
                            }else{
                                Attribute attribute = new Attribute(null, field[0]);
                                arg.add(attribute);
                            }
                        }
                    }else{
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
                }else{
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

            }else if(res[0].equalsIgnoreCase("select")){
                return selectparse(res);

            }else {
                return false;
            }

        } catch (Exception e) {
            return false;
        }

        return true;

    }

    private boolean selectparse(String[] res) {
        select = new NodeProfile();
        key_word.add("select");

        int f_index = -1;
        int w_index = -1;
        int o_index = -1;
        int d_index = -1;

        for(int i = 1; i < res.length; i++){
            if(res[i].equalsIgnoreCase("distinct")){
                d_index = i;
            }

            if(res[i].equalsIgnoreCase("from")){
                f_index = i;
            }

            if(res[i].equalsIgnoreCase("where")){
                w_index = i;
            }

            if(res[i].equalsIgnoreCase("order")){
                o_index = i;
            }
        }

        if(d_index == 1){
            select.distinct = true;
        }else if(d_index >= 0){
            return false;
        }

        if(w_index > 0 && o_index > 0 && w_index > o_index) {
            System.out.print("Order can not be front of Where!");
            return false;
        }

        if(f_index < 0){
            System.out.print("No from!!");
            return false;
        }
        StringBuilder sb = new StringBuilder();
        if(d_index > 0){
            for(int i = 2; i < f_index; i++){
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
        }else{

            for(int i = 1; i < f_index; i++){
                sb.append(res[i]+" ");
            }
            String []arg_s = sb.toString().split(",");

            if(arg_s[0].trim().equalsIgnoreCase("*")){
                if(arg_s.length == 1) {
                    select.arguments.add("*");
                }
                else{
                    return false;
                }

            }else{
                for(int i = 0; i < arg_s.length; i++){
                    arg_s[i] = arg_s[i].trim();
                    select.arguments.add(arg_s[i]);
                }
            }
        }

        sb = new StringBuilder();
        if(w_index > 0){
            select.where = true;
            for(int i = f_index + 1; i < w_index; i++){
                sb.append(res[i]+" ");
            }
            String[] tables = sb.toString().split(",");
            for(int i = 0; i < tables.length; i++){
                tables[i] = tables[i].trim();
                select.tables.add(tables[i]);
            }

            sb = new StringBuilder();
            if(o_index > 0) {
                select.order = true;
                for(int i = w_index + 1; i < o_index; i++){
                    sb.append(res[i]+" ");
                }
                select.where_clause = TreeGenerator.generate(sb.toString());

                if(!res[o_index+1].equalsIgnoreCase("by")){
                    System.out.print("order by false!!");
                    return false;
                }

                sb = new StringBuilder();
                for(int i = o_index+2; i < res.length; i++){
                    sb.append(res[i]+" ");
                }
                select.o_clause = sb.toString();
            }else{
                for(int i = w_index + 1; i < res.length; i++){
                    sb.append(res[i]+" ");
                }
                select.where_clause = TreeGenerator.generate(sb.toString());
            }
        }else{
            if(o_index > 0){
                if(!res[o_index + 1].equalsIgnoreCase("by")){
                    System.out.print("order by false!!");
                    return false;
                }

                select.order = true;
                for(int i = f_index + 1; i < o_index; i++){
                    sb.append(res[i]+" ");
                }
                String[] tables = sb.toString().split(",");
                for(int i = 0; i < tables.length; i++){
                    tables[i] = tables[i].trim();
                    select.tables.add(tables[i]);
                }

                sb = new StringBuilder();
                for(int i = o_index + 2; i < res.length; i++){
                    sb.append(res[i]+" ");
                }
                select.o_clause = sb.toString();
            }else{
                for(int i = f_index + 1; i < res.length; i++){
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
