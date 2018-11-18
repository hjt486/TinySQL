import java.io.*;
import java.util.*;
/*
Jiatai Han & Qifan Li
11/2018

Main Interface
*/

public class test {
    public static void main(String[] args){
        Scanner select = new Scanner(System.in);
        System.out.println("|====Welcome to the TinySQL Interpreter====|");
        System.out.println("|       CSCE 608 Fall 2018 Project 2       |");
        System.out.println("|      Author: Jiatai Han && Qifan Li      |");
        System.out.println("|==========================================|");
        System.out.println("|-----Please enter the number to begin-----|");
        System.out.println("|1. Input from a text file                 |");
        System.out.println("|2. Enter the query manually               |");
        System.out.println("|3. Quit                                   |");
        System.out.println("|==========================================|");
        while (true) {
            System.out.print("Please enter the number from the list: ");
            String level1 = select.next();
            switch (level1) {
                case "1":
                    try {
                        System.out.println("|==========================================|");
                        System.out.println("|=============Input From a File============|");
                        System.out.println("|==========================================|");
                        System.out.println("Please put the text file in the program folder,");
                        System.out.print("Then enter the file name (with extension) here:");
                        //  Begin read file name
                        Scanner input = new Scanner(System.in);
                        String file_name = input.next();
                        File file = new File(file_name);
                        Scanner scanner = new Scanner(new FileInputStream(file));
                        // Create executor and read from file and execute line by line
                        Executor executor = new Executor();
                        int i = 0;
                        long time_begin = System.currentTimeMillis();
                        while (scanner.hasNextLine()) {
                            i++;
                            executor.execute(scanner.nextLine());
                        }
                        System.out.println("Time consumed:");
                        System.out.println(System.currentTimeMillis()- time_begin +"ms");
                        System.out.println("|=================Completed================|");
                    }
                    catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    continue;
                case "2":
                    System.out.println("|==========================================|");
                    System.out.println("|============Enter Query by Line===========|");
                    System.out.println("|==========================================|");
                    System.out.println("|===========Enter \"exit\" to quit===========|");
                    while (true) {
                        System.out.print("TinySQL>");
                        Scanner scan = new Scanner(System.in);
                        String query = "";
                        if (scan.hasNextLine()) {
                            query = scan.nextLine();
                        }
                        else {
                            query = scan.next();
                        }
                        System.out.println(query);
                        if (query.equalsIgnoreCase("exit")) {
                            break;
                        }
                        else {
                            Executor executor = new Executor();
                            executor.execute(query);
                        }
                    }
                    continue;
                case "3":
                    System.out.println("You selected \"Quit\".");
                    System.out.println("Thank You and See You Soon!");
                    return;
                default:
                    continue;
            }
        }
    }
}
