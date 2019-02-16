package edu.jsu.mcis;

import java.sql.*;
import java.util.ArrayList;
import org.json.simple.*;

public class DatabaseTest {

    public static void main(String[] args) {
        
        JSONArray array = getJSONData();
        System.out.println("\nCONVERSION RESULTS (DATABASE TO JSON)");
        System.out.println("=====================================");
        System.out.println(array);
        System.out.println();
        
    }
    
    public static JSONArray getJSONData() {
        
        Connection conn = null;
        PreparedStatement pstSelect = null, pstUpdate = null;
        ResultSet resultset = null;
        ResultSetMetaData metadata = null;
        JSONArray records = new JSONArray();
        
        String query, value;
        ArrayList<String> key = new ArrayList<>();
        
        boolean hasresults;
        int resultCount, columnCount, updateCount = 0;
        
        try {
            
            /* Identify the Server */
            
            String server = ("jdbc:mysql://localhost/p2_test");
            String username = "root";
            String password = "CS310";
            //System.out.println("Connecting to " + server + "...");
            
            /* Load the MySQL JDBC Driver */
            
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            
            /* Open Connection */

            conn = DriverManager.getConnection(server, username, password);

            /* Test Connection */
            
            if (conn.isValid(0)) {
                
                /* Prepare Select Query */
                
                query = "SELECT * FROM people";
                pstSelect = conn.prepareStatement(query);
                
                hasresults = pstSelect.execute();       
                
                /* Put every rows in JSONObject */
                
                while ( hasresults || pstSelect.getUpdateCount() != -1 ) {

                    if ( hasresults ) {
                        
                        /* Get ResultSet Metadata */
                        
                        resultset = pstSelect.getResultSet();
                        metadata = resultset.getMetaData();
                        columnCount = metadata.getColumnCount();
                        
                        /* Get Column Names; Append them in an ArraList "key" */
                        
                        for (int i = 2; i <= columnCount; i++) {
                            key.add(metadata.getColumnLabel(i));
                        }
                        
                        /* Get Data; Print as Table Rows */
                        
                        while(resultset.next()) {
                            
                            /* Begin Next ResultSet Row; Loop Through ResultSet 
                            Columns; Append to jsonObject */
                            
                            JSONObject object = new JSONObject();

                            for (int i = 2; i <= columnCount; i++) {
                                
                                JSONObject jsonObject = new JSONObject();
                                value = resultset.getString(i);
                                
                                if (resultset.wasNull()) {
                                    jsonObject.put(key.get(i-2), "NULL");
                                    jsonObject.toJSONString();
                                }

                                else {
                                    jsonObject.put(key.get(i-2), value);
                                    jsonObject.toString();
                                }
                                
                                object.putAll(jsonObject);

                            }
                            records.add(object);

                        }
                        
                    }

                    else {

                        resultCount = pstSelect.getUpdateCount();  

                        if ( resultCount == -1 ) {
                            break;
                        }

                    }
                    
                    /* Check for More Data */

                    hasresults = pstSelect.getMoreResults();

                }
                
            }
            
            System.out.println();
            
            /* Close Database Connection */
            
            conn.close();
            
        }
        
        catch (Exception e) {
            System.err.println(e.toString());
        }
        
        /* Close Other Database Objects */
        
        finally {
            
            if (resultset != null) { try { resultset.close(); resultset = null; } catch (Exception e) {} }
            
            if (pstSelect != null) { try { pstSelect.close(); pstSelect = null; } catch (Exception e) {} }
            
            if (pstUpdate != null) { try { pstUpdate.close(); pstUpdate = null; } catch (Exception e) {} }
            
        }
        
        return records;
            
    }
        
}
    
    
