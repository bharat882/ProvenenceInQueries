package Package1;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.*;
import java.util.regex.Pattern;

public class Main  {
	
	static Connection connection = DatabaseConnection.getConnection();

    public static void main(String[] args) {
 
    if (connection != null) {
        System.out.println("Database connection successful");
        try {
        	
     //   	String inputQuery = "SELECT product_type, region_name FROM products, regions where origin_region = regions.region_id;";
       	
        //	String inputQuery = "select supplier_name from suppliers, routes where supplier= suppliers.supplier_id AND region_to = 5;";
        			
           String inputQuery = "SELECT region_name from routes, regions, products WHERE product_type= 'ELECTRONICS' AND routes.product = products.product_id AND routes.region_from = regions.region_id;";

      //  	String inputQuery = "SELECT product, product_type FROM routes, products WHERE product =  products.product_id AND region_from = 1;";
                      
            String condition = getCondition(inputQuery);
            ArrayList<String> columns = getColumns(inputQuery);
            ArrayList<String> relations = getRelations(inputQuery);      
            ArrayList<String> datatypes = getDatatypes(columns);
           
            // ann_res stores the key value pair where key hold the columns and pair hold the annotations
            Map<String, String> ann_res = new HashMap<>();
            
            // colms stores a part of columns for the query
            // SAMPLE VALUE: product AS COLM0, product_type AS COLM1, 
            
            String colms="";
            Iterator itr = columns.iterator();
            int counter2=0;
            while(itr.hasNext())
            {
            	colms+=itr.next()+" AS COLM"+counter2+", ";
            	counter2++;
            }
            
            
            // EXECUTING THE INPUT QUERY
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(inputQuery);
            Set<String> seenRows = new HashSet<>();
            String reln = "";
            String tabl = "";
        	Iterator itr2 = relations.iterator();
        	int counter=0;
        	while(itr2.hasNext())
        	{
        		String temp = (String)itr2.next();
        		reln+=temp;
        		tabl+=temp+".ann AS ANN"+counter+" ";
        		counter++;
        		
        		if(itr2.hasNext())
        		{
        			reln+=", ";
        			tabl+=", ";
        		}
        		
        	}
        	// ann_query_template hold a part of the query to find the annotations
        	// Sample value for ann_query_template = SELECT product AS COLM0, product_type AS COLM1, routes.ann AS ANN0 , products.ann AS ANN1  FROM routes, products WHERE product =  products.product_id AND region_from = 1 AND 
        	
    		String ann_query_template = "SELECT "+colms+tabl+" FROM "+reln+" WHERE "+condition;       		
    		ann_query_template = ann_query_template.substring(0,ann_query_template.length()-1);
    		ann_query_template+=" AND ";
    		
    		// annQueries holds the complete query i.e. ann_query_template + remaining conditions
    		
            while(resultSet.next())
            {
            	String uniqueKey="";
            	String ann_query = ann_query_template;
            	String annRes = "";
            	

            	for (int i = 0; i<datatypes.size();i++) {

            		if(datatypes.get(i).equals("integer"))
            		{
            			int value = resultSet.getInt(columns.get(i));
            			annRes+=String.valueOf(value) + " ,";
            			uniqueKey += value;
            			ann_query+= columns.get(i)+" = "+value+" AND ";

            		}
            		else if(datatypes.get(i).equals("character varying"))
            		{
            			
            			String value = resultSet.getString(columns.get(i));
            			annRes+=value + " ,";
            			uniqueKey +=value;
            			ann_query +=columns.get(i)+" = '"+value+"' AND ";

            		} 

            	}
            	
            	String final_ann_query = ann_query.substring(0, (ann_query.length()-5));
            	
            	if(seenRows.contains(uniqueKey)) {
            		continue;
            	}
            	else
            	{
            		seenRows.add(uniqueKey);
            	}
            	
            	Statement statement2 = connection.createStatement();
            	ResultSet resultSet2 = statement2.executeQuery(final_ann_query);
            	while(resultSet2.next()) {
            		annRes += " ( ";
            		for(int i=0;i<relations.size();i++)
            		{               			
            			annRes+= resultSet2.getString("ANN"+i) + " ";
            		}
            		annRes += " ) ";
            	}
            	
            	System.out.println(annRes + "\n");
            	resultSet2.close();
            	                			

            }
       
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    } else {
        System.out.println("Failed to connect to the database.");
    }
}
private static ArrayList<String> getDatatypes(ArrayList<String> columns) {
	
	
	ArrayList<String> datatypes = new ArrayList<>();
	int i=0;
    for (String columnName : columns) {
 
    	String query = "SELECT DATA_TYPE AS DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '"+columnName+"' ;";

        Statement statement;
		try {
			statement = connection.createStatement();
			 ResultSet resultSet = statement.executeQuery(query);
			 while(resultSet.next()) {
	                String dataType = resultSet.getString("DATA_TYPE");
	                datatypes.add(dataType);
	        }
		}
			 catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}       	
    }		
    return datatypes;
		}
	
private static String getCondition(String inputQuery) {

    Pattern pattern3 = Pattern.compile("WHERE\\s+(.*)",Pattern.CASE_INSENSITIVE);
    Matcher matcher3 = pattern3.matcher(inputQuery);

    if (matcher3.find()) {
        String conditions = matcher3.group(1);
        return conditions;
    }
    return null;
}
private static ArrayList<String> getRelations(String inputQuery) {
	
	Pattern pattern2 = Pattern.compile("FROM\\s+(.*?)(?:\\s+WHERE|$)",Pattern.CASE_INSENSITIVE);
	Matcher matcher2 = pattern2.matcher(inputQuery);
	
	ArrayList<String> relationNames = new ArrayList<>();
	
	if (matcher2.find()) {
	String tables = matcher2.group(1);
	String[] tableNameArray = tables.split(",\\s*");
	for (String tableName : tableNameArray) {
	 relationNames.add(tableName.trim());
	}
	}

	return relationNames;
	
}

public static ArrayList getColumns(String inputQuery) {
	
	 String regex = "SELECT\\s+(.+?)\\s+FROM";
	Pattern pattern1 = Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
	
	 Matcher matcher1= pattern1.matcher(inputQuery);

    ArrayList<String> columnNames = new ArrayList<>();

    if (matcher1.find()) {
        String columns = matcher1.group(1);
        String[] columnNameArray = columns.split(",\\s*");
        for (String columnName : columnNameArray) {
            columnNames.add(columnName.trim());
        }
    }

    return columnNames;
   	
}
}


