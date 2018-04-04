import java.sql.*;
import org.apache.derby.jdbc.ClientDriver;

public class FindMajors {
    public static void main(String[] args) {
	    	//CS 4432
		// for our purposes we have a configuration where
		// the first argument is set to 'drama' to verify the changing of student 'amy'
		// from "ChangeMajor.java". When testing not in IntelliJ you may have to alter
		// a configuration. We are unsure of what would happen if the files were ran in Eclipse or another IDE
		String major = args[0];
		System.out.println("Here are the " + major + " majors");
		System.out.println("Name\tGradYear");

		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new ClientDriver();
			String url = "jdbc:derby://localhost/studentdb";
			conn = d.connect(url, null);

			// Step 2: execute the query
			Statement stmt = conn.createStatement();
			String qry = "select sname, gradyear "
			           + "from student, dept "
			           + "where did = majorid "
			           + "and dname = '" + major + "'";
			ResultSet rs = stmt.executeQuery(qry);

			// Step 3: loop through the result set
			while (rs.next()) {
				String sname = rs.getString("sname");
				int gradyear = rs.getInt("gradyear");
				System.out.println(sname + "\t" + gradyear);
			}
			rs.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			// Step 4: close the connection
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
