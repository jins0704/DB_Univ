package DatabaseTest;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.*;
import java.util.Scanner;

public class sqlTest2 {
	

	public static void main(String[] args) throws  SQLException{
		Connection conn = null;
		Statement stmt = null; 
		PreparedStatement pstm = null;
		ResultSet rs = null;
		Scanner scan = null;
		
	  try {
		  scan = new Scanner(System.in);
		  System.out.println("SQL Programming Test");
		  
		  //connection 
		  System.out.println("Connection PostgreSQL database");
		  conn = DriverManager.getConnection("jdbc:postgresql://localhost/mydb", "fbduddn97","1357");
		  System.out.println("Success Connection");
		  stmt = conn.createStatement();
		  
		  System.out.println("Continue? (Enter 1 for continue)");
		  scan.nextLine();
		  
		  /*
		  stmt.executeUpdate("drop Table Student");
		  stmt.executeUpdate("drop Table Apply");
		  stmt.executeUpdate("drop Table College");
		  */
			
		  //create table
		  System.out.println("Creating College,Student,Apply relations");	
			  
		  stmt.executeUpdate("create table Student (sId int,sName varchar(20),GPA numeric(2,1),sizeHS int)");
		  stmt.executeUpdate("create table Apply (sID int,cName varchar(20),major varchar(20), decision char)");
		  stmt.executeUpdate("create table College (cName varchar(20),state varchar(20), enrollment int)"); 
		  //insert tuples
		  System.out.println("Inserting tuples to College, Student, Apply relations \n");
	 
		  stmt.executeUpdate("insert into College values ('Stanford', 'CA', 15000)"); 
		  stmt.executeUpdate("insert into College values ('Berkeley', 'CA', 36000)");
		  stmt.executeUpdate("insert into College values ('MIT', 'MA', 10000)");
		  stmt.executeUpdate("insert into College values ('Cornell', 'NY', 21000)");
			  
		  stmt.executeUpdate("insert into Student values (123, 'Amy', 3.9, 1000)");
		  stmt.executeUpdate("insert into Student values (234, 'Bob', 3.6,1500 )");
		  stmt.executeUpdate("insert into Student values (345, 'Craig', 3.5, 500)");
		  stmt.executeUpdate("insert into Student values (456, 'Doris', 3.9, 1000)");
		  stmt.executeUpdate("insert into Student values (567, 'Edward', 2.9, 2000)");
		  stmt.executeUpdate("insert into Student values (678, 'Fay', 3.8, 200)");
		  stmt.executeUpdate("insert into Student values (789, 'Gary', 3.4, 800)");
 		  stmt.executeUpdate("insert into Student values (987, 'Helen', 3.7, 800)");
		  stmt.executeUpdate("insert into Student values (876, 'Irene', 3.9, 400)");
		  stmt.executeUpdate("insert into Student values (765, 'Jay', 2.9, 1500)");
		  stmt.executeUpdate("insert into Student values (654, 'Amy', 3.9, 1000)");
	      stmt.executeUpdate("insert into Student values (543, 'Craig', 3.4, 2000)");
			  
		  stmt.executeUpdate("insert into Apply values (123, 'Stanford', 'CS', 'Y')");
		  stmt.executeUpdate("insert into Apply values (123, 'Stanford', 'EE', 'N')");
		  stmt.executeUpdate("insert into Apply values (123, 'Berkeley', 'CS', 'Y')");
		  stmt.executeUpdate("insert into Apply values (123, 'Cornell', 'EE', 'Y')");
		  stmt.executeUpdate("insert into Apply values (234, 'Berkeley', 'biology', 'N')");
		  stmt.executeUpdate("insert into Apply values (345, 'MIT', 'bioengineering', 'Y')");
		  stmt.executeUpdate("insert into Apply values (345, 'Cornell', 'bioengineering', 'N')");
		  stmt.executeUpdate("insert into Apply values (345, 'Cornell', 'CS', 'Y')");
		  stmt.executeUpdate("insert into Apply values (345, 'Cornell', 'EE', 'N')");
		  stmt.executeUpdate("insert into Apply values (678, 'Stanford', 'history', 'Y')");
		  stmt.executeUpdate("insert into Apply values (987, 'Stanford', 'CS', 'Y')");
		  stmt.executeUpdate("insert into Apply values (987, 'Berkeley', 'CS', 'Y')");
		  stmt.executeUpdate("insert into Apply values (876, 'Stanford', 'CS', 'N')");
		  stmt.executeUpdate("insert into Apply values (876, 'MIT', 'biology', 'Y')");
		  stmt.executeUpdate("insert into Apply values (876, 'MIT', 'marine biology', 'N')");
		  stmt.executeUpdate("insert into Apply values (765, 'Stanford', 'history', 'Y')");
		  stmt.executeUpdate("insert into Apply values (765, 'Cornell', 'history', 'N')");
		  stmt.executeUpdate("insert into Apply values (765, 'Cornell', 'psychology', 'Y')");
		  stmt.executeUpdate("insert into Apply values (543, 'MIT', 'CS', 'N')");
			  
		  
			  
			  
		  //Query1
		  System.out.println("Trigger test1");
		  
		  
		  stmt.execute("CREATE OR REPLACE FUNCTION test1()\n"
		  		+ "		  returns trigger\n"
		  		+ "		  AS $$\n"
		  		+ "		  BEGIN\n"
		  		+ "		  	insert into Apply values(New.sID, 'Stanford', 'geology', null); \n"
		  		+ "		  	insert into Apply values(New.sID,'MIT', 'biology', null); \n"
		  		+ "		  	return New;\n"
		  		+ "		  END; $$\n"
		  		+ "		  language 'plpgsql';");
		  
		  stmt.execute("create trigger R1 after insert on Student for each row when (New.GPA > 3.3 and New.GPA <= 3.6) execute procedure test1()");
		  
		  stmt.executeUpdate("insert into Student values(111,'Kevin',3.5,1000)");
		  stmt.executeUpdate("insert into Student values(222,'Lori',3.8,1000)");
		  
		  System.out.println("<Student table>");
		  rs = stmt.executeQuery("select * from Student");
		   while(rs.next()) {
				 String sID=rs.getString("sID");
				 String sName=rs.getString("sName");
				 String GPA=rs.getString("GPA");
				 String sizeHS=rs.getString("SizeHS");
				 
				 System.out.println('(' + sID + " " + sName + " " + GPA + " "+sizeHS +')');
			 }
		   
		   System.out.println("\n");
		   System.out.println("<Apply Table>");
		   rs = stmt.executeQuery("select * from Apply");
			  while(rs.next()) {
					 String sID=rs.getString("sID");
					 String cName=rs.getString("cName");
					 String major=rs.getString("major");
					 String decision=rs.getString("decision");
					 
					 System.out.println('(' + sID + " " + cName + " " + major + " "+decision +')');
				 }
		  		  
		  System.out.println("Continue? (Enter 1 for continue)");
		  scan.nextLine();
		  
		  
		 
		  
		  //Query2
		  System.out.println("Trigger test2");
		  
		  stmt.execute("CREATE OR REPLACE FUNCTION test2()\n"
			  		+ "		  returns trigger\n"
			  		+ "		  AS $$\n"
			  		+ "		  BEGIN\n"
			  		+ "		  	update Apply set CName = New.cName where CName = Old.cName; \n"
			  		+ "		  	return New;\n"
			  		+ "		  END; $$\n"
			  		+ "		  language 'plpgsql';");
			   
		 stmt.execute("create trigger R3 after update of cName on College for each row execute procedure test2()");
		 
		 
		 stmt.executeUpdate("update College set cName='The Farm' where cName='Stanford'");
		 

		 System.out.println("<College table>");  	
		 rs = stmt.executeQuery("select * from College");
		 while(rs.next()) {
			 String cName=rs.getString("cName");
			 String state=rs.getString("state");
			 String enrollment=rs.getString("enrollment");
			 
			 System.out.println('(' + cName + " " + state + " " + enrollment +')');
		 }
		  
		  System.out.println("Continue? (Enter 1 for continue)");
		  scan.nextLine();
		  
		  
		  
		  
		  ///Table reset
		  System.out.println("Table reset");
		  stmt.executeUpdate("drop Table Student");
		  stmt.executeUpdate("drop Table Apply");
		  stmt.executeUpdate("drop Table College");			 
			  
		  //create table	  
		  stmt.executeUpdate("create table Student (sId int,sName varchar(20),GPA numeric(2,1),sizeHS int)");
		  stmt.executeUpdate("create table Apply (sID int,cName varchar(20),major varchar(20), decision char)");
	      stmt.executeUpdate("create table College (cName varchar(20),state varchar(20), enrollment int)"); 
		  
	      //insert tuples
		  stmt.executeUpdate("insert into College values ('Stanford', 'CA', 15000)"); 
		  stmt.executeUpdate("insert into College values ('Berkeley', 'CA', 36000)");
		  stmt.executeUpdate("insert into College values ('MIT', 'MA', 10000)");
		  stmt.executeUpdate("insert into College values ('Cornell', 'NY', 21000)");
			  
		  stmt.executeUpdate("insert into Student values (123, 'Amy', 3.9, 1000)");
		  stmt.executeUpdate("insert into Student values (234, 'Bob', 3.6,1500 )");
		  stmt.executeUpdate("insert into Student values (345, 'Craig', 3.5, 500)");
		  stmt.executeUpdate("insert into Student values (456, 'Doris', 3.9, 1000)");
		  stmt.executeUpdate("insert into Student values (567, 'Edward', 2.9, 2000)");
		  stmt.executeUpdate("insert into Student values (678, 'Fay', 3.8, 200)");
		  stmt.executeUpdate("insert into Student values (789, 'Gary', 3.4, 800)");
		  stmt.executeUpdate("insert into Student values (987, 'Helen', 3.7, 800)");
		  stmt.executeUpdate("insert into Student values (876, 'Irene', 3.9, 400)");
		  stmt.executeUpdate("insert into Student values (765, 'Jay', 2.9, 1500)");
		  stmt.executeUpdate("insert into Student values (654, 'Amy', 3.9, 1000)");
	      stmt.executeUpdate("insert into Student values (543, 'Craig', 3.4, 2000)");
			  
		  stmt.executeUpdate("insert into Apply values (123, 'Stanford', 'CS', 'Y')");
		  stmt.executeUpdate("insert into Apply values (123, 'Stanford', 'EE', 'N')");
		  stmt.executeUpdate("insert into Apply values (123, 'Berkeley', 'CS', 'Y')");
		  stmt.executeUpdate("insert into Apply values (123, 'Cornell', 'EE', 'Y')");
		  stmt.executeUpdate("insert into Apply values (234, 'Berkeley', 'biology', 'N')");
		  stmt.executeUpdate("insert into Apply values (345, 'MIT', 'bioengineering', 'Y')");
		  stmt.executeUpdate("insert into Apply values (345, 'Cornell', 'bioengineering', 'N')");
		  stmt.executeUpdate("insert into Apply values (345, 'Cornell', 'CS', 'Y')");
		  stmt.executeUpdate("insert into Apply values (345, 'Cornell', 'EE', 'N')");
		  stmt.executeUpdate("insert into Apply values (678, 'Stanford', 'history', 'Y')");
		  stmt.executeUpdate("insert into Apply values (987, 'Stanford', 'CS', 'Y')");
		  stmt.executeUpdate("insert into Apply values (987, 'Berkeley', 'CS', 'Y')");
		  stmt.executeUpdate("insert into Apply values (876, 'Stanford', 'CS', 'N')");
		  stmt.executeUpdate("insert into Apply values (876, 'MIT', 'biology', 'Y')");
	      stmt.executeUpdate("insert into Apply values (876, 'MIT', 'marine biology', 'N')");
		  stmt.executeUpdate("insert into Apply values (765, 'Stanford', 'history', 'Y')");
		  stmt.executeUpdate("insert into Apply values (765, 'Cornell', 'history', 'N')");
		  stmt.executeUpdate("insert into Apply values (765, 'Cornell', 'psychology', 'Y')");
		  stmt.executeUpdate("insert into Apply values (543, 'MIT', 'CS', 'N')");
			  
		  			  
		  scan.nextLine();
			  
		  //Query3
		  System.out.println("View test1");
		  stmt.execute("create view CSaccept as select sID, cName from Apply where major='CS' and decision='Y';");
		  rs = stmt.executeQuery("select * from CSaccept");
		  System.out.println("<CSaccept table>");
		  while(rs.next()) {
			  	 String sID=rs.getString("sID");	
				 String cName=rs.getString("cName");
			
				 
				 System.out.println('('+sID+ " " + cName +')');
			 }	  
		  System.out.println("Continue? (Enter 1 for continue)");
		  scan.nextLine();
		  
		  
		   
		  //Query4
		  System.out.println("View test2");
		  
		  stmt.execute("CREATE OR REPLACE FUNCTION CSacceptDelete()\n"
			  		+ "		  returns trigger\n"
			  		+ "		  AS $$\n"
			  		+ "		  BEGIN\n"
			  		+ "		  	delete from Apply where sID=Old.sID and cName=Old.cName and major= 'CS' and decision='Y'; \n"
			  		+ "		  	return New;\n"
			  		+ "		  END; $$\n"
			  		+ "		  language 'plpgsql';");
			   
		 stmt.execute("create trigger CacceptDelete instead of delete on CSaccept for each row execute procedure CSacceptDelete()");
		 stmt.executeUpdate("delete from CSaccept where sId=123;");
		 
		 rs = stmt.executeQuery("select * from CSaccept");
		 System.out.println("<CSaccept table>");
		 while(rs.next()) {
			  	 String sID=rs.getString("sID");	
				 String cName=rs.getString("cName");
			
				 
				 System.out.println('('+sID+ " " + cName +')');
		 }
		 System.out.println("\n");
		 System.out.println("<Apply table>");
		 rs = stmt.executeQuery("select * from Apply");
		  while(rs.next()) {
				 String sID=rs.getString("sID");
				 String cName=rs.getString("cName");
				 String major=rs.getString("major");
				 String decision=rs.getString("decision");
				 
				 System.out.println('(' + sID + " " + cName + " " + major + " "+decision +')');
			 }
		 
		  System.out.println("Continue? (Enter 1 for continue)");
		  scan.nextLine();
		  
		  
		  
		  
		  //Query5
		  System.out.println("View test3");
		  
		  stmt.execute("CREATE OR REPLACE FUNCTION CSacceptUpdate()\n"
			  		+ "		  returns trigger\n"
			  		+ "		  AS $$\n"
			  		+ "		  BEGIN\n"
			  		+ "		  	Update Apply set cName=New.cName where sId=Old.sId and cName=Old.cName and major='CS' and decision='Y'; \n"
			  		+ "		  	return New;\n"
			  		+ "		  END; $$\n"
			  		+ "		  language 'plpgsql';");
			   
		 stmt.execute("create trigger CSacceptUpdate instead of Update on CSaccept for each row execute procedure CSacceptUpdate()");
		 stmt.executeUpdate("update CSaccept set cName='CMU' where sID=345;");
		 
		 rs = stmt.executeQuery("select * from CSaccept");
		 System.out.println("<CSaccept table>");
		 while(rs.next()) {
			  	 String sID=rs.getString("sID");	
				 String cName=rs.getString("cName");
			
				 
				 System.out.println('('+sID+ " " + cName +')');
		 }
		 System.out.println("\n");
		 System.out.println("<Apply table>");
		 rs = stmt.executeQuery("select * from Apply");
		  while(rs.next()) {
				 String sID=rs.getString("sID");
				 String cName=rs.getString("cName");
				 String major=rs.getString("major");
				 String decision=rs.getString("decision");
				 
				 System.out.println('(' + sID + " " + cName + " " + major + " "+decision +')');
			 }
		  
		  
		  System.out.println("Continue? (Enter 1 for continue)");
		  scan.nextLine();
		  
		  
	  }catch(Exception e) 
	  {
		  System.out.println(e.getMessage());
	  }finally {
		  try {
			  if (stmt != null){
				  stmt.close();
			  }
			  if(conn!=null) {
				  conn.close();
			  }
			  
		  }catch(Exception e) {
			  System.out.println(e.getMessage());
		  }
	  }		
		
		
	}

}

