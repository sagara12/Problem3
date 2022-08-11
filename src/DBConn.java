import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DBConn {

    public static void main(String[] args) {
        final String driver = "org.mariadb.jdbc.Driver";
        final String DB_IP = "codingtest.brique.kr";
        final String DB_PORT = "3306";
        final String DB_NAME = "employees";
        final String DB_URL =
                "jdbc:mariadb://" + DB_IP + ":" + DB_PORT + "/" + DB_NAME;

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(DB_URL, "codingtest", "12brique!@");
            if (conn != null) {
                System.out.println("DB 접속 성공");
            }

        } catch (ClassNotFoundException e) {
            System.out.println("드라이버 로드 실패");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("DB 접속 실패");
            e.printStackTrace();
        }

        try {
            String sql = "SELECT e.emp_no, e.first_name, e.last_name, e.gender, e.hire_date, dept.dept_name, t.title, s.max_salary\n" +
                    "from employees as e\n" +
                    "join titles as t\n" +
                    "Left outer join\n" +
                    "(SELECT d2.dept_name, d1.emp_no from  dept_emp as d1 Inner join departments as d2 \n" +
                    "on d1.dept_no = d2.dept_no \n" +
                    ")as dept on dept.emp_no = e.emp_no\n" +
                    "Left outer join\n" +
                    "(select max(salary) as 'max_salary', emp_no\n" +
                    "FROM salaries\n" +
                    "GROUP BY emp_no\n" +
                    ")AS s ON e.emp_no = s.emp_no\n" +
                    "where e.emp_no = t.emp_no\n" +
                    "AND e.hire_date >='2000-01-01'";

            pstmt = conn.prepareStatement(sql);

            rs = pstmt.executeQuery();
            List<Employee> eList = new ArrayList<>();
            Employee employee1 = new Employee();

            employee1.setEmp_no("Emp_no");
            employee1.setFirst_name("First_name");
            employee1.setLast_name("Last_name");
            employee1.setGender("Gender");
            employee1.setHire_date("Hire_date");
            employee1.setDepartment("Department");
            employee1.setTitle("Title");
            employee1.setMax_salary("Max_salary");
            eList.add(employee1);
            
            while (rs.next()) {
                
                Employee employee =  new Employee();
                employee.setEmp_no(rs.getString(1));
                employee.setFirst_name(rs.getString(2));
                employee.setLast_name(rs.getString(3));
                employee.setGender(rs.getString(4));
                employee.setHire_date(rs.getString(5));
                employee.setDepartment(rs.getString(6));
                employee.setTitle(rs.getString(7));
                employee.setMax_salary(rs.getString(8));
                eList.add(employee);
            }

            for (int i = 0 ; i < eList.size() ; i++){

                System.out.println(eList.get(i).getEmp_no() + "  :  " + eList.get(i).getFirst_name() + "  :  " + eList.get(i).getLast_name() + "  :  " + eList.get(i).getGender() + "   :   " + eList.get(i).getHire_date() + "   :   " + eList.get(i).getDepartment() + "    :    " + eList.get(i).getTitle() + "    :    "  + eList.get(i).getMax_salary());

            }

            System.out.println("  ");
            System.out.println("  ");
            System.out.println("종료하려면 exit를 입력 하세요");
            Scanner input = new Scanner(System.in);
            String esc = String.valueOf(input.hasNextLine());




            if (esc.equals("exit")) {
                System.exit(0);
            }

            
        } catch (SQLException e) {
            System.out.println("error: " + e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }

                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}
