package DatabaseJS;

import java.io.*;
import java.sql.*;
import java.util.Scanner;

import java.sql.SQLException;

public class SqlTest3 {

    public static void main(String[] args) throws SQLException {
        try {
            Scanner scan = new Scanner(System.in); System.out.println("SQL Programming Test");
            // JDBC를 이용해 PostgreSQL 서버 및 데이터베이스 연결
            Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/postgres1","postgres","1357");
            Statement state = conn.createStatement();
            ResultSet result = null;

            /*	
            state.executeUpdate("drop Table ParentOf");
            state.executeUpdate("drop Table Flight");
            */

            state.executeUpdate("create table ParentOf(parent varchar(20), child varchar(20));");
            state.executeUpdate(" create table Flight(orig varchar(20), dest varchar(20), airline varchar(20), cost int);");

            state.executeUpdate("insert into ParentOf values('Alice', 'Carol');");
            state.executeUpdate("insert into ParentOf values('Bob', 'Carol');");
            state.executeUpdate("insert into ParentOf values('Carol', 'Dave');");
            state.executeUpdate("insert into ParentOf values('Carol', 'George');");
            state.executeUpdate("insert into ParentOf values('Dave', 'Mary');");
            state.executeUpdate("insert into ParentOf values('Eve', 'Mary');");
            state.executeUpdate("insert into ParentOf values('Mary', 'Frank');");

            state.executeUpdate("insert into Flight values('A', 'ORD', 'United', 200);");
            state.executeUpdate("insert into Flight values('ORD', 'B', 'American', 100);");
            state.executeUpdate("insert into Flight values('A', 'PHX', 'Southwest', 25);");
            state.executeUpdate("insert into Flight values('PHX', 'LAS', 'Southwest', 30);");
            state.executeUpdate("insert into Flight values('LAS', 'CMH', 'Frontier', 60);");
            state.executeUpdate("insert into Flight values('CMH', 'B', 'Frontier', 60);");
            state.executeUpdate("insert into Flight values('A', 'B', 'JetBlue', 195);");

            //Recursive Test 1
            System.out.print("\n");
            System.out.println("Recursive test 1");
            System.out.print("\n");

            result = state.executeQuery("with recursive \n"
                    + " Ancestor(a,d) as (select parent as a, child as d from ParentOf \n "
                    + "     union \n "
                    + "     select Ancestor.a, ParentOf.child as d \n "
                    + "     from Ancestor, ParentOf \n "
                    + "     where Ancestor.d = ParentOf.parent)"
                    + "select a from Ancestor where d ='Mary'");

            while(result.next()) {
                System.out.println("Ancestor : " + result.getString("a"));
            }

            System.out.print("\n");
            System.out.println("Continue? (Enter 1 for continue)"); scan.nextLine();

            //Recursive test 2
            System.out.println("Recursive test 2");
            System.out.print("\n");

            result = state.executeQuery("with recursive \n"
                    + " Route(orig,dest,total) as \n"
                    + "     (select orig,dest,cost as total from Flight \n "
                    + "     union \n "
                    + "     select R.orig, F.dest, cost+total as total \n "
                    + "     from Route R, Flight F \n "
                    + "     where R.dest = F.orig)"
                    +"select * from Route \n"
                    +"where orig = 'A' and dest = 'B'");

            while(result.next()) {
                System.out.print("orig : " + result.getString("orig"));
                System.out.print("   dest : " + result.getString("dest"));
                System.out.println("   total : " + result.getString("total"));
            }

            System.out.print("\n");
            System.out.println("Continue? (Enter 1 for continue)"); scan.nextLine();

            //Recursive test 3
            System.out.println("Recursive test 3");
            System.out.print("\n");

            result = state.executeQuery("with recursive \n"
                    + " TOB(orig,total) as \n"
                    + "     (select orig, cost as total from Flight where dest = 'B' \n "
                    + "     union \n "
                    + "     select F.orig, cost+total as total \n "
                    + "     from Flight F, TOB TB \n "
                    + "     where F.dest = TB.orig)"
                    + "select min(total) as mintotal from TOB where orig = 'A'");

            while(result.next()) {
                System.out.println("min : " + result.getString("mintotal"));
            }

            System.out.print("\n");
            System.out.println("Continue? (Enter 1 for continue)"); scan.nextLine();

            /*state.executeUpdate("drop Table Sales");
            state.executeUpdate("drop Table Customer");
            state.executeUpdate("drop Table Item");
            state.executeUpdate("drop Table Store");*/

            state.executeUpdate("create table Sales(storeID varchar(20), itemID varchar(20), custID varchar(20), price int);");
            state.executeUpdate("create table Customer(custID varchar(20), cName varchar(20), gender char, age int);");
            state.executeUpdate("create table Item(itemID varchar(20), category varchar(20), color varchar(20));");
            state.executeUpdate("create table Store(storeID varchar(20), city varchar(20), county varchar(20), state char(2));");

            state.executeUpdate("insert into Customer values('cust1', 'Amy', 'F', 20);");
            state.executeUpdate("insert into Customer values('cust2', 'Bob', 'M', 21);");
            state.executeUpdate("insert into Customer values('cust3', 'Craig', 'M', 25);");
            state.executeUpdate("insert into Customer values('cust4', 'Doris', 'F', 22);");

            state.executeUpdate("insert into Item values('item1', 'Tshirt', 'blue');");
            state.executeUpdate("insert into Item values('item2', 'Jacket', 'blue');");
            state.executeUpdate("insert into Item values('item3', 'Tshirt', 'red');");
            state.executeUpdate("insert into Item values('item4', 'Jacket', 'blue');");
            state.executeUpdate("insert into Item values('item5', 'Jacket', 'red');");

            state.executeUpdate("insert into Store values('store1', 'Palo Alto', 'Santa Clara', 'CA');");
            state.executeUpdate("insert into Store values('store2', 'Mountain View', 'Santa Clara', 'CA');");
            state.executeUpdate("insert into Store values('store3', 'Menio Park', 'San Mateo', 'CA');");
            state.executeUpdate("insert into Store values('store4', 'Belmont', 'San Mateo', 'CA');");
            state.executeUpdate("insert into Store values('store5', 'Seattle', 'King', 'CA');");
            state.executeUpdate("insert into Store values('store6', 'Redmond', 'King', 'CA');");

            state.executeUpdate("insert into Sales values('store1', 'item1', 'cust1', 10);");
            state.executeUpdate("insert into Sales values('store1', 'item1', 'cust2', 15);");
            state.executeUpdate("insert into Sales values('store1', 'item1', 'cust3', 20);");
            state.executeUpdate("insert into Sales values('store1', 'item1', 'cust3', 25);");
            state.executeUpdate("insert into Sales values('store1', 'item2', 'cust1', 30);");
            state.executeUpdate("insert into Sales values('store1', 'item2', 'cust2', 35);");
            state.executeUpdate("insert into Sales values('store1', 'item2', 'cust3', 40);");
            state.executeUpdate("insert into Sales values('store1', 'item2', 'cust2', 45);");
            state.executeUpdate("insert into Sales values('store1', 'item3', 'cust1', 50);");
            state.executeUpdate("insert into Sales values('store1', 'item3', 'cust1', 55);");
            state.executeUpdate("insert into Sales values('store2', 'item3', 'cust2', 60);");
            state.executeUpdate("insert into Sales values('store2', 'item1', 'cust2', 65);");
            state.executeUpdate("insert into Sales values('store2', 'item2', 'cust3', 70);");
            state.executeUpdate("insert into Sales values('store2', 'item2', 'cust3', 75);");
            state.executeUpdate("insert into Sales values('store2', 'item2', 'cust4', 80);");
            state.executeUpdate("insert into Sales values('store2', 'item2', 'cust4', 85);");
            state.executeUpdate("insert into Sales values('store2', 'item2', 'cust1', 90);");
            state.executeUpdate("insert into Sales values('store2', 'item2', 'cust1', 95);");
            state.executeUpdate("insert into Sales values('store2', 'item2', 'cust1', 95);");
            state.executeUpdate("insert into Sales values('store2', 'item2', 'cust2', 90);");

            state.executeUpdate("insert into Sales values('store3', 'item2', 'cust2', 85);");
            state.executeUpdate("insert into Sales values('store3', 'item2', 'cust2', 80);");
            state.executeUpdate("insert into Sales values('store3', 'item2', 'cust3', 75);");
            state.executeUpdate("insert into Sales values('store3', 'item2', 'cust3', 70);");
            state.executeUpdate("insert into Sales values('store3', 'item3', 'cust3', 65);");
            state.executeUpdate("insert into Sales values('store3', 'item3', 'cust2', 60);");
            state.executeUpdate("insert into Sales values('store3', 'item3', 'cust2', 55);");
            state.executeUpdate("insert into Sales values('store3', 'item3', 'cust2', 50);");
            state.executeUpdate("insert into Sales values('store3', 'item3', 'cust3', 45);");
            state.executeUpdate("insert into Sales values('store3', 'item3', 'cust3', 40);");

            state.executeUpdate("insert into Sales values('store4', 'item3', 'cust1', 35);");
            state.executeUpdate("insert into Sales values('store4', 'item3', 'cust1', 30);");
            state.executeUpdate("insert into Sales values('store4', 'item3', 'cust2', 25);");
            state.executeUpdate("insert into Sales values('store4', 'item3', 'cust2', 20);");
            state.executeUpdate("insert into Sales values('store4', 'item3', 'cust2', 15);");
            state.executeUpdate("insert into Sales values('store4', 'item3', 'cust2', 10);");
            state.executeUpdate("insert into Sales values('store4', 'item4', 'cust3', 15);");
            state.executeUpdate("insert into Sales values('store4', 'item4', 'cust3', 20);");
            state.executeUpdate("insert into Sales values('store4', 'item4', 'cust3', 25);");
            state.executeUpdate("insert into Sales values('store4', 'item4', 'cust3', 30);");

            state.executeUpdate("insert into Sales values('store5', 'item4', 'cust4', 35);");
            state.executeUpdate("insert into Sales values('store5', 'item4', 'cust4', 40);");
            state.executeUpdate("insert into Sales values('store5', 'item4', 'cust4', 45);");
            state.executeUpdate("insert into Sales values('store5', 'item4', 'cust4', 50);");
            state.executeUpdate("insert into Sales values('store5', 'item4', 'cust1', 55);");
            state.executeUpdate("insert into Sales values('store5', 'item4', 'cust1', 60);");
            state.executeUpdate("insert into Sales values('store5', 'item4', 'cust1', 65);");
            state.executeUpdate("insert into Sales values('store5', 'item4', 'cust2', 70);");
            state.executeUpdate("insert into Sales values('store5', 'item5', 'cust1', 75);");
            state.executeUpdate("insert into Sales values('store5', 'item5', 'cust2', 80);");

            state.executeUpdate("insert into Sales values('store6', 'item5', 'cust3', 85);");
            state.executeUpdate("insert into Sales values('store6', 'item5', 'cust3', 90);");
            state.executeUpdate("insert into Sales values('store6', 'item2', 'cust3', 95);");
            state.executeUpdate("insert into Sales values('store6', 'item2', 'cust4', 90);");
            state.executeUpdate("insert into Sales values('store6', 'item3', 'cust4', 85);");
            state.executeUpdate("insert into Sales values('store6', 'item3', 'cust4', 80);");
            state.executeUpdate("insert into Sales values('store6', 'item4', 'cust4', 75);");
            state.executeUpdate("insert into Sales values('store6', 'item4', 'cust4', 70);");
            state.executeUpdate("insert into Sales values('store6', 'item5', 'cust4', 65);");
            state.executeUpdate("insert into Sales values('store6', 'item5', 'cust4', 60);");

            //OLAP test 1
            System.out.println("OLAP test 1");
            System.out.print("\n");

            result = state.executeQuery("select storeID, itemID, custID, sum(price) \n"
                    + "from Sales \n"
                    + "group by cube(storeID,itemID,custID) \n"
                    + "order by storeID, itemID, custID");

            while(result.next()) {
                System.out.print("storeID : " + result.getString("storeID"));
                System.out.print("   itemID : " + result.getString("itemID"));
                System.out.print("   custID : " + result.getString("custID"));
                System.out.println("   sum : " + result.getString("sum"));
            }

            System.out.print("\n");
            System.out.println("Continue? (Enter 1 for continue)"); scan.nextLine();

            //OLAP test 2
            System.out.println("OLAP test 2");
            System.out.print("\n");

            result = state.executeQuery("select storeID, itemID, custID, sum(price) \n"
                    + "from Sales F \n"
                    + "group by itemID, cube(storeID, custID) \n"
                    + "order by storeID, itemID, custID");

            while(result.next()) {
                System.out.print("storeID : " + result.getString("storeID"));
                System.out.print("   itemID : " + result.getString("itemID"));
                System.out.print("   custID : " + result.getString("custID"));
                System.out.println("   sum : " + result.getString("sum"));
            }

            System.out.print("\n");
            System.out.println("Continue? (Enter 1 for continue)"); scan.nextLine();

            //OLAP test 3
            System.out.println("OLAP test 3");
            System.out.print("\n");

            result = state.executeQuery("select storeID, itemID, custID, sum(price) \n"
                    + "from Sales F \n"
                    + "group by rollup(storeID, itemID, custID) \n"
                    + "order by storeID, itemID, custID");

            while(result.next()) {
                System.out.print("storeID : " + result.getString("storeID"));
                System.out.print("   itemID : " + result.getString("itemID"));
                System.out.print("   custID : " + result.getString("custID"));
                System.out.println("   sum : " + result.getString("sum"));
            }

            System.out.println("Continue? (Enter 1 for continue)"); scan.nextLine();
        } catch(SQLException ex) {
            throw ex; }
    }
}