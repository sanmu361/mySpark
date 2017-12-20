import java.sql.*;

/**
 * ${DESCRIPTION}
 *
 * @author yansen
 * @create 2017-12-05 12:07
 **/
public class HiveData {
    public void hiveData() throws SQLException{
        String url = "jdbc:hive2://node3:10000/";
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection(url,"root","123456");
        Statement stmt = conn.createStatement();
        String sql = "select * from table_area_test";
        System.out.println("Running"+sql);
        ResultSet res = stmt.executeQuery(sql);
        while(res.next()){
            System.out.println("id: "+res.getInt(1)+"\tname: "+res.getString(2)+"\tage: "+res.getString(3));
        }
    }
}
