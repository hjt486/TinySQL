import java.io.*;
import java.util.*;
import storageManager.*;

/*
Jiatai Han & Qifan Li
11/2018

Main Interface
*/
public class Main {
    public static void main(String[] args){

        Scanner select = new Scanner(System.in);
        boolean control = true;
        System.out.println("|====Welcome to the TinySQL Interpreter====|");
        System.out.println("|       CSCE 608 Fall 2018 Project 2       |");
        System.out.println("|      Author: Jiatai Han && Qifan Li      |");

        while (control) {
            System.out.println("|==========================================|");
            System.out.println("|-----Please enter the number to begin-----|");
            System.out.println("|1. Input from a text file                 |");
            System.out.println("|2. Enter the query manually               |");
            System.out.println("|3. Quit                                   |");
            System.out.println("|==========================================|");
            System.out.print("Please enter the number from the list: ");
            String level = select.nextLine();
            switch (level) {
                case "1":
                    try {
                        System.out.println("|==========================================|");
                        System.out.println("|=============Input From a File============|");
                        System.out.println("|==========================================|");
                        System.out.println("Please put the text file in the program folder,");
                        System.out.print("Then enter the file name (with extension) here:");
                        //  Begin read file name
                        Scanner input = new Scanner(System.in);
                        String file_name = input.nextLine();
                        File file = new File(file_name);
                        Scanner scanner = new Scanner(new FileInputStream(file));
                        // Create executor and read from file and execute line by line
                        Executor run = new Executor();
                        long time_begin = System.currentTimeMillis();
                        while (scanner.hasNextLine()) {
                            run.execute(scanner.nextLine());
                        }
                        System.out.println("Time consumed:");
                        System.out.println(System.currentTimeMillis()- time_begin +"ms");
                        System.out.println("|=================Completed================|");
                    }
                    catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    //control = false;
                    break;
                case "2":
                    System.out.println("|==========================================|");
                    System.out.println("|============Enter Query by Line===========|");
                    System.out.println("|==========================================|");
                    System.out.println("|===========Enter \"exit\" to quit===========|");
                    Executor run = new Executor();
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
                            control = false;
                            break;
                        } else {
                            //Executor run = new Executor();
                            run.execute(query);
                        }
                    }
                    break;
                case "3":
                    System.out.println("You selected \"Quit\".");
                    System.out.println("Thank You and See You Soon!");
                    control = false;
                    break;
                default:
                    System.out.println("Unrecognizable choice. Choose again:");
                    break;
                    //level = select.nextLine();
            }
        }
    }
}
