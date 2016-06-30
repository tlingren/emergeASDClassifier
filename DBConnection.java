package org.apache.ctakes.emerge.rules;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {

	   protected static Connection getConnection(String driver, String url, String user, String pass) throws Exception {
		      registerDriver(driver);
		      Connection connection = null;
		      try {
		         connection = DriverManager.getConnection( url, user, pass );
		      } catch ( SQLException sqlE ) {
		         // thrown by Connection.prepareStatement(..) and getTotalRowCount(..)
		         System.err.println( "Could not establish connection to " + url + " as " + user );
		         System.err.println( sqlE.getMessage() );
		         System.exit( 1 );
		      }
		      return connection;
		   }
	   private static void registerDriver(String name) throws Exception {
		      String driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver" ;
		      if(name!=null) {
		    	  driverName = name;
		      }
		         Driver driver = (Driver)Class.forName( driverName ).newInstance();
		         DriverManager.registerDriver( driver );
		   }	   
}
