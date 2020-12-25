import java.io.*;
import java.sql.*;
import java.util.Scanner;

public class SqlTest {
    public static void main(String[] args) throws SQLException
    {
        try
        {
            Scanner scan = new Scanner(System.in);
            System.out.println("SQL Programming Test");

            System.out.println("Connecting PostgreSQL database");
            // JDBC를 이용해 PostgreSQL 서버 및 데이터베이스 연결
            Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/postgres","postgres","wlstjr753");
            Statement state = conn.createStatement();
            ResultSet result = null;

            // 3개 테이블 생성: Create table문 이용
            System.out.println("Creating College, Student, Apply relations");
            state.executeUpdate("create table College (cName varchar(20),state varchar(20), enrollment int)");
            state.executeUpdate("create table Student (sId int,sName varchar(20),GPA numeric(2,1),sizeHS int)");
            state.executeUpdate("create table Apply (sID int,cName varchar(20),major varchar(20), decision char)");

            System.out.println("Inserting tuples to College, Student, Apply relations");
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
            // 3개 테이블에 튜플 생성: Insert문 이용

            System.out.println("Continue? (Enter 1 for continue)");
            scan.nextLine();
            System.out.println("Query 1");
            // Query 1을 실행: Select문 이용
            // Query 처리 결과는 적절한 Print문을 이용해 Display
            result = state.executeQuery("select *from college");
            while(result.next()){
                System.out.println("cName : " + result.getString("cName") + " " + ", state : " + result.getString("state") + " " + ", enrollment : " + result.getString("enrollment"));
            }

            // Query 2 ~ Query 6에 대해 Query 1과 같은 방식으로 실행: Select문 이용
            // Query 처리 결과는 적절한 Print문을 이용해 Display
            System.out.println("Continue? (Enter 1 for continue)");
            scan.nextLine();
            System.out.println("Query 2");
            result = state.executeQuery("select * from Student");
            while(result.next()) {
                System.out.println("sID : " + result.getString("sID") + " " + ", sName : " + result.getString("sName") + " " + ", GPA : " + result.getString("sizeHS"));
            }

            System.out.println("Continue? (Enter 1 for continue)");
            scan.nextLine();
            System.out.println("Query 3");
            result = state.executeQuery("select * from Apply");
            while(result.next()) {
                System.out.println("sID : " + result.getString("sID") + " " + ", cName : " + result.getString("cName") + " " + ", major : " + result.getString("major") + " " + ", decision : " + result.getString("decision"));
            }

            System.out.println("Continue? (Enter 1 for continue)");
            scan.nextLine();
            System.out.println("Query 4");
            result = state.executeQuery("select * from Student S1 where(select count(*) from Student S2 where S2.sID <> S1.sID and S2.GPA=S1.GPA)=(select count(*) from Student S2 where S2.sID <> S1.sID and S2.sizeHS=S1.sizeHS)");
            while(result.next()) {
                System.out.println("sID : " + result.getString("sID") + " " + ", sName : " + result.getString("sName") + " " + ", GPA : " + result.getString("sizeHS"));
            }

            System.out.println("Continue? (Enter 1 for continue)");
            scan.nextLine();
            System.out.println("Query 5");
            result = state.executeQuery("select Student.sID, sName, count(distinct cName)\n" +
                    "from Student, Apply\n" +
                    "where Student.sID = Apply.sID\n" +
                    "group by Student.sID, sName;");
            while(result.next()) {
                System.out.println("Student.sID : " + result.getString("sID") + " " + ", sName : " + result.getString("sName") + " " + ", count(distinct cName) : " + result.getString("count"));
            }

            System.out.println("Continue? (Enter 1 for continue)");
            scan.nextLine();
            System.out.println("Query 6");
            result = state.executeQuery("select major\n" +
                    "from Student, Apply\n" +
                    "where Student.sID = Apply.sID\n" +
                    "group by major\n" +
                    "having max(GPA) < (select avg(GPA) from Student);");
            while(result.next()) {
                System.out.println("major : " + result.getString("major"));
            }

            System.out.println("Continue? (Enter 1 for continue)");
            scan.nextLine();
            System.out.println("Query 7");
            // Query 7을 실행: Select문 이용
            // 사용자에게 sizeHS, major, cName 값으로 1000, CS, Stanford 입력 받음
            // 입력 받은 값을 넣어 Query를 처리하고
            // 결과는 적절한 Print문을 이용해 Display
            System.out.print("sizeHS 입력 : ");
            String mySizeHS= scan.next();
            System.out.print("major 입력 : ");
            String myMajor=scan.next();
            System.out.print("cName 입력 : ");
            String mycName=scan.next();
            String a = "select sName, GPA\n from Student join Apply\n on Student.sID = Apply.sID\n where sizeHS <";
            String b = " and major = ";
            String c = " and cName = ";
            result = state.executeQuery(a + mySizeHS + b +"'"+ myMajor +"'" + c + "'" + mycName + "'");
            while(result.next()) {
                System.out.println("sName : " + result.getString("sName")+ " " + ", GPA : " + result.getString("GPA"));
            }
        } catch(SQLException ex)
        {
            throw ex;
        }
    }
}
