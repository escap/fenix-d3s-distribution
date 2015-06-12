package org.fao.ess.uneca.d3s;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataSource {
    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
    }

    private String url,usr,psw;

    public void init(String url, String usr, String psw) {
        this.url = url;
        this.usr = usr;
        this.psw = psw;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, usr, psw);
    }

}
