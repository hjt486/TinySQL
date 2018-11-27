/*
Jiatai Han & Qifan Li
11/2018

This class defines attribute (field) object,
with name and its data type;
Should be just INT and STR20.
*/

public class Attribute {
    String name;
    String type;

    public Attribute(String type, String name) {
        this.name = name;
        this.type = type;
    }
}
