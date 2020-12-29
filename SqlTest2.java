package DatabaseJS;

import java.io.*;
import java.sql.*;
import java.util.Scanner;

import java.sql.SQLException;

public class SqlTest2 {
	
	public static void main(String[] args) throws SQLException {
       try {
            Scanner scan = new Scanner(System.in); System.out.println("SQL Programming Test");
            System.out.println("Connecting PostgreSQL database");
           // JDBC를 이용해 PostgreSQL 서버 및 데이터베이스 연결
            Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/postgres1","postgres","1357");
            Statement state = conn.createStatement();
            ResultSet result = null;

            /*
            state.executeUpdate("drop Table Student");
	        state.executeUpdate("drop Table Apply");
	        state.executeUpdate("drop Table College");
	        */
	        
	        
	        // 3개 테이블 생성: Create table문 이용
	        state.executeUpdate("create table College (cName varchar(20),state varchar(20), enrollment int)");
	        state.executeUpdate("create table Student (sId int,sName varchar(20),GPA numeric(2,1),sizeHS int)");
	        state.executeUpdate("create table Apply (sID int,cName varchar(20),major varchar(20), decision char)");
	        
	        state.executeUpdate("insert into College values ('Stanford', 'CA', 15000)");
	        state.executeUpdate("insert into College values ('Berkeley', 'CA', 36000)");
	        state.executeUpdate("insert into College values ('MIT', 'MA', 10000)");	
	        state.executeUpdate("insert into College values ('Cornell', 'NY', 21000)");
	        
	        state.executeUpdate("insert into Student values (123, 'Amy', 3.9, 1000)");
	        state.executeUpdate("insert into Student values (234, 'Bob', 3.6,1500 )");
	        state.executeUpdate("insert into Student values (345, 'Craig', 3.5, 500)");
	        state.executeUpdate("insert into Student values (456, 'Doris', 3.9, 1000)");
	        state.executeUpdate("insert into Student values (567, 'Edward', 2.9, 2000)");
	        state.executeUpdate("insert into Student values (678, 'Fay', 3.8, 200)");
	        state.executeUpdate("insert into Student values (789, 'Gary', 3.4, 800)");
	        state.executeUpdate("insert into Student values (987, 'Helen', 3.7, 800)");
	        state.executeUpdate("insert into Student values (876, 'Irene', 3.9, 400)");
	        state.executeUpdate("insert into Student values (765, 'Jay', 2.9, 1500)");
	        state.executeUpdate("insert into Student values (654, 'Amy', 3.9, 1000)");
	        state.executeUpdate("insert into Student values (543, 'Craig', 3.4, 2000)");

	        state.executeUpdate("insert into Apply values (123, 'Stanford', 'CS', 'Y')");
	        state.executeUpdate("insert into Apply values (123, 'Stanford', 'EE', 'N')");
	        state.executeUpdate("insert into Apply values (123, 'Berkeley', 'CS', 'Y')");
	        state.executeUpdate("insert into Apply values (123, 'Cornell', 'EE', 'Y')");
	        state.executeUpdate("insert into Apply values (234, 'Berkeley', 'biology', 'N')");
	        state.executeUpdate("insert into Apply values (345, 'MIT', 'bioengineering', 'Y')");
	        state.executeUpdate("insert into Apply values (345, 'Cornell', 'bioengineering', 'N')");
	        state.executeUpdate("insert into Apply values (345, 'Cornell', 'CS', 'Y')");
	        state.executeUpdate("insert into Apply values (345, 'Cornell', 'EE', 'N')");
	        state.executeUpdate("insert into Apply values (678, 'Stanford', 'history', 'Y')");
	        state.executeUpdate("insert into Apply values (987, 'Stanford', 'CS', 'Y')");
	        state.executeUpdate("insert into Apply values (987, 'Berkeley', 'CS', 'Y')");
	        state.executeUpdate("insert into Apply values (876, 'Stanford', 'CS', 'N')");
	        state.executeUpdate("insert into Apply values (876, 'MIT', 'biology', 'Y')");
	        state.executeUpdate("insert into Apply values (876, 'MIT', 'marine biology', 'N')");
	        state.executeUpdate("insert into Apply values (765, 'Stanford', 'history', 'Y')");
	        state.executeUpdate("insert into Apply values (765, 'Cornell', 'history', 'N')");
	        state.executeUpdate("insert into Apply values (765, 'Cornell', 'psychology', 'Y')");
	        state.executeUpdate("insert into Apply values (543, 'MIT', 'CS', 'N')");

	//Query1
	        System.out.print("\n");
	        System.out.println("Trigger test 1");
	        System.out.print("\n");
	        
	        state.execute("CREATE OR REPLACE FUNCTION test1() returns trigger as $$\n"
	        		+ "begin\n"
	        		+ "	    insert into Apply values(New.sID, 'Stanford', 'geology', null); \n"
	        		+ "	 	insert into Apply values(New.sID,'MIT', 'biology', null); \n"
	        		+ "	 	return New;\n"
	        		+ "end; $$\n"
	        		+ "language 'plpgsql';");

	        state.execute("create trigger R1 \n"
	        			+ "after insert on Student \n"
	                    + "for each row when (New.GPA > 3.3 and New.GPA <= 3.6)"
	                    + "execute procedure test1()");

	        state.executeUpdate("insert into Student values(111,'Kevin',3.5,1000)");
	        state.executeUpdate("insert into Student values(222,'Lori',3.8,1000)");
	        
	        result = state.executeQuery("select *from Student");

	        System.out.println("select *from Student");
	        
	        while(result.next()) {
	        	System.out.print("sID : " + result.getString("sID"));
	        	System.out.print("   sName : " + result.getString("sName"));
	        	System.out.print("   GPA : " + result.getString("GPA"));
	        	System.out.println("   sizeHS : " + result.getString("sizeHS"));
	        }
	        
	        System.out.print("\n");
	        System.out.println("select *from Apply");
	        
	        result = state.executeQuery("select *from Apply");
	        
	        while(result.next()) {
	        	System.out.print("sID : " + result.getString("sID"));
	        	System.out.print("	cName : " + result.getString("cName"));
	        	System.out.print("   major : " + result.getString("major"));
	        	System.out.println("	decision : " + result.getString("decision"));
	        }
	        
	        System.out.print("\n");
	        System.out.println("Continue? (Enter 1 for continue)"); scan.nextLine();
	        System.out.println("Trigger test 2");
	        System.out.print("\n");

	// Query2
	        state.execute("CREATE OR REPLACE FUNCTION test2() returns trigger as $$\n"
	        		+ "begin\n"
	        		+ "	    update Apply set CName = New.cName where CName = Old.cName; \n"
	        		+ "	 	return New;\n"
	        		+ "end; $$\n"
	        		+ "language 'plpgsql';");
	        state.execute("create trigger R3 after update of cName on College for each row execute procedure test2()");
	        state.executeUpdate("update College set cName='The Farm' where cName='Stanford'");

	        result = state.executeQuery("select * from College");

	        System.out.println("select * from College");
	        
	        while(result.next()) {
	        	System.out.print("cName : " + result.getString("cName"));
	        	System.out.print("   state : " + result.getString("state"));
	        	System.out.println("   enrollment : " + result.getString("enrollment"));
	        }
	        
	        System.out.print("\n");
	        System.out.println("Continue? (Enter 1 for continue)"); scan.nextLine();
	          
	        state.executeUpdate("drop Table Student");
	        state.executeUpdate("drop Table Apply");
	        state.executeUpdate("drop Table College");

	        // 3개 테이블 생성: Create table문 이용
	       
	        state.executeUpdate("create table College (cName varchar(20),state varchar(20), enrollment int)");
	        state.executeUpdate("create table Student (sId int,sName varchar(20),GPA numeric(2,1),sizeHS int)");
	        state.executeUpdate("create table Apply (sID int,cName varchar(20),major varchar(20), decision char)");
	        
	       
	        state.executeUpdate("insert into College values ('Stanford', 'CA', 15000)");
	        state.executeUpdate("insert into College values ('Berkeley', 'CA', 36000)");
	        state.executeUpdate("insert into College values ('MIT', 'MA', 10000)");
	        state.executeUpdate("insert into College values ('Cornell', 'NY', 21000)");

	        state.executeUpdate("insert into Student values (123, 'Amy', 3.9, 1000)");
	        state.executeUpdate("insert into Student values (234, 'Bob', 3.6,1500 )");
	        state.executeUpdate("insert into Student values (345, 'Craig', 3.5, 500)");
	        state.executeUpdate("insert into Student values (456, 'Doris', 3.9, 1000)");
	        state.executeUpdate("insert into Student values (567, 'Edward', 2.9, 2000)");
	        state.executeUpdate("insert into Student values (678, 'Fay', 3.8, 200)");
	        state.executeUpdate("insert into Student values (789, 'Gary', 3.4, 800)");
	        state.executeUpdate("insert into Student values (987, 'Helen', 3.7, 800)");
	        state.executeUpdate("insert into Student values (876, 'Irene', 3.9, 400)");
	        state.executeUpdate("insert into Student values (765, 'Jay', 2.9, 1500)");
	        state.executeUpdate("insert into Student values (654, 'Amy', 3.9, 1000)");
	        state.executeUpdate("insert into Student values (543, 'Craig', 3.4, 2000)");

	        state.executeUpdate("insert into Apply values (123, 'Stanford', 'CS', 'Y')");
	        state.executeUpdate("insert into Apply values (123, 'Stanford', 'EE', 'N')");
	        state.executeUpdate("insert into Apply values (123, 'Berkeley', 'CS', 'Y')");
	        state.executeUpdate("insert into Apply values (123, 'Cornell', 'EE', 'Y')");
	        state.executeUpdate("insert into Apply values (234, 'Berkeley', 'biology', 'N')");
	        state.executeUpdate("insert into Apply values (345, 'MIT', 'bioengineering', 'Y')");
	        state.executeUpdate("insert into Apply values (345, 'Cornell', 'bioengineering', 'N')");
	        state.executeUpdate("insert into Apply values (345, 'Cornell', 'CS', 'Y')");
	        state.executeUpdate("insert into Apply values (345, 'Cornell', 'EE', 'N')");
	        state.executeUpdate("insert into Apply values (678, 'Stanford', 'history', 'Y')");
	        state.executeUpdate("insert into Apply values (987, 'Stanford', 'CS', 'Y')");
	        state.executeUpdate("insert into Apply values (987, 'Berkeley', 'CS', 'Y')");
	        state.executeUpdate("insert into Apply values (876, 'Stanford', 'CS', 'N')");
	        state.executeUpdate("insert into Apply values (876, 'MIT', 'biology', 'Y')");
	        state.executeUpdate("insert into Apply values (876, 'MIT', 'marine biology', 'N')");
	        state.executeUpdate("insert into Apply values (765, 'Stanford', 'history', 'Y')");
	        state.executeUpdate("insert into Apply values (765, 'Cornell', 'history', 'N')");
	        state.executeUpdate("insert into Apply values (765, 'Cornell', 'psychology', 'Y')");
	        state.executeUpdate("insert into Apply values (543, 'MIT', 'CS', 'N')");
	        
	// Query 3
	        System.out.println("View test 1");
	        System.out.print("\n");
	        state.execute("create view CSaccept as select sID, cName from Apply where major='CS' and decision='Y';");
	        result = state.executeQuery("select * from CSaccept");
	        
	        System.out.println("select * from CSaccept");
	  	  	
	        while(result.next()) {
	            System.out.print("sID : " + result.getString("sID"));
                System.out.println("   cName : " + result.getString("cName"));
	        }
	        
	        System.out.print("\n");
	        System.out.println("Continue? (Enter 1 for continue)"); scan.nextLine();            
	            
	// Query 4
	        System.out.println("View test 2");
	        System.out.print("\n");
	        state.execute("CREATE OR REPLACE FUNCTION CSacceptDelete() returns trigger as $$\n"
	        		+ "begin\n"
	        		+ "	    delete from Apply where sID=Old.sID and cName=Old.cName and major= 'CS' and decision='Y'; \n"
	        		+ "	 	return New;\n"
	        		+ "end; $$\n"
	        		+ "language 'plpgsql';");
	        state.execute("create trigger CacceptDelete instead of delete on CSaccept for each row execute procedure CSacceptDelete()");
	        state.executeUpdate("delete from CSaccept where sId=123;");

	        result = state.executeQuery("select * from CSaccept");
	        System.out.println("select * from CSaccept");
	         
	        while(result.next()) {
	        	System.out.print("sID : " + result.getString("sID"));
	        	System.out.println("   cName : " + result.getString("cName"));
	        }
	        System.out.print("\n");
	        result = state.executeQuery("select * from Apply");
	        System.out.println("select * from Apply");
	 		  
	        while(result.next()) {
	        	System.out.print("sID : " + result.getString("sID"));
	            System.out.print("	cName : " + result.getString("cName"));
	            System.out.print("	major : " + result.getString("major"));
	            System.out.println("	decision : " + result.getString("decision"));
	        }
	        
	        System.out.println("Continue? (Enter 1 for continue)"); scan.nextLine();
	
	          
	// Query 5
	        System.out.println("View test 3");
	        System.out.print("\n");
	        state.execute("CREATE OR REPLACE FUNCTION CSacceptUpdate() returns trigger as $$ \n"
	        		+ "begin \n"
	        		+ "	    Update Apply set cName=New.cName where sId=Old.sId and cName=Old.cName and major='CS' and decision='Y'; \n"
	        		+ "	 	return New; \n"
	        		+ "end; $$ \n"
	        		+ "language 'plpgsql';");
	        state.execute("create trigger CSacceptUpdate instead of Update on CSaccept for each row execute procedure CSacceptUpdate()");
	        state.executeUpdate("update CSaccept set cName='CMU' where sID=345;");
	        
	        result = state.executeQuery("select * from CSaccept");
	        
	        while(result.next()) {
	               System.out.print("sID : " + result.getString("sID"));
	               System.out.println("	cName : " + result.getString("cName"));
	 		}
	        
	        System.out.print("\n");
	        result = state.executeQuery("select * from Apply");
			
	        System.out.println("select * from Apply");
			
	        while(result.next()) {
	        	System.out.print("sID : " + result.getString("sID"));
	        	System.out.print("	cName : " + result.getString("cName"));
	        	System.out.print("	major : " + result.getString("major"));
	        	System.out.println("	decision : " + result.getString("decision"));
	 		 }
		
	        System.out.println("Continue? (Enter 1 for continue)"); scan.nextLine();
	        } catch(SQLException ex) {
	        	throw ex; }
	    }
	}
