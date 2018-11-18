/**
 * Created by lsr on 2016/11/24.
 */
public class Statement {
    public static String[] load(String s){
        s.replace("\\s{1,}"," ");
        String[] res=s.split(" ");
        return res;
    }
}
