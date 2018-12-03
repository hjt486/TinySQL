import java.io.*;
import java.util.*;
/*
Jiatai Han & Qifan Li
11/2018

TinySQL Interface
*/

public class TinySQL {
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
            System.out.println("|3. Exit                                   |");
            System.out.println("|==========================================|");
            System.out.print("Please enter the number from the list: ");
            String level = select.nextLine();
            switch (level) {
                case "1":
                    try {
                        System.out.println("");
                        System.out.println("|==========================================|");
                        System.out.println("|=============Input From a File============|");
                        System.out.println("|==========================================|");
                        System.out.println("Please put the text file in the program folder,");
                        System.out.println("then enter the file name with extension (eg. test.txt)");
                        System.out.println("or enter \"exit\" to return to main manual.");
                        System.out.print("File>");
                        //  Begin read file name
                        Scanner input = new Scanner(System.in);
                        String file_name = input.nextLine();
                        // Create executor and read from file and execute line by line
                        if (file_name.equalsIgnoreCase("exit")) {
                            System.out.println("");
                            control = true;
                            break;
                        } else {
                            File file = new File(file_name);
                            Scanner scanner = new Scanner(new FileInputStream(file));
                            Interpreter run = new Interpreter();
                            long time_begin = System.currentTimeMillis();
                            while (scanner.hasNextLine()) {
                                run.execute(scanner.nextLine());
                            }
                            System.out.print("Time consumed: ");
                            System.out.println(System.currentTimeMillis()- time_begin +"ms");
                            System.out.print("Total Disk I/O consumed: ");
                            System.out.println(run.disk.getDiskIOs());
                            System.out.println("");
                        }
                    }
                    catch (FileNotFoundException e) {
                        e.printStackTrace();
                }
                    break;
                case "2":
                    System.out.println("");
                    System.out.println("|==========================================|");
                    System.out.println("|============Enter Query by Line===========|");
                    System.out.println("|==========================================|");
                    System.out.println("Enter \"exit\" to return to main manual.");
                    Interpreter run = new Interpreter();
                    while (true) {
                        long diskio = run.disk.getDiskIOs();
                        System.out.print("TinySQL>");
                        Scanner scan = new Scanner(System.in);
                        String query = "";
                        long time_begin = 0;
                        if (scan.hasNextLine()) {
                            time_begin = System.currentTimeMillis();
                            query = scan.nextLine();
                        }
                        else {
                            time_begin = System.currentTimeMillis();
                            query = scan.next();
                        }


                        if (query.equalsIgnoreCase("exit")) {
                            System.out.println("");
                            control = true;
                            break;
                        } else {
                            run.execute(query);
                            System.out.print("Time consumed: ");
                            System.out.println(System.currentTimeMillis()- time_begin +"ms");
                            System.out.print("Disk I/O consumed: ");
                            long diskio_consumed = run.disk.getDiskIOs() - diskio;
                            System.out.println(diskio_consumed);
                            System.out.print("Total Disk I/O consumed: ");
                            System.out.println(run.disk.getDiskIOs());
                            System.out.println("");
                        }
                    }
                    break;
                case "3":
                    System.out.println("");
                    System.out.println("You selected \"Exit\".");
                    System.out.println("Thank You and See You Soon!");
                    control = false;
                    break;
                default:
                    System.out.println("");
                    System.out.println("Unrecognizable choice, please choose again:");
                    control = true;
                    System.out.println("");
                    break;
            }
        }
    }
}
