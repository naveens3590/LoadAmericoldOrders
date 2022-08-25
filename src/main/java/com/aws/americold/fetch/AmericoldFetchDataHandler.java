/**
* Fetch records from the americold database and check if any order are available or not
*
* @author Naveen Kumar
*/
package com.aws.americold.fetch;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
public class AmericoldFetchDataHandler implements RequestHandler<Object, Object> {
	private static Statement stmt = null;
	private static ResultSet results;
	
	
	public static Object loadOrderData() {
		Object JsonOutput=null;
		// read data from database
		String sql_select = "Select * From americold where Order_Status='Ordered' and Created_At >=Date_sub(sysdate(),interval 1 hour) ";
		try {
			Connection conn = DBconnection.createNewDBconnection();

			stmt = conn.createStatement();
			results = stmt.executeQuery(sql_select);

			List<Americold> americoldList = new ArrayList<Americold>();
			while (results.next()) {
				Americold americoldObj = new Americold();
				americoldObj.setOrder_Id(Integer.valueOf(results.getString("Order_Id")));
				americoldObj.setCustomer_Id((String.valueOf(results.getString("Customer_Id"))));
				americoldObj.setOrder_Status((String.valueOf(results.getString("Order_Status"))));
				americoldObj.setProduct_Id((String.valueOf(results.getString("Product_Id"))));
				americoldObj.setProduct_Name((String.valueOf(results.getString("Product_Name"))));
				americoldObj.setQuantity((String.valueOf(results.getString("Quantity"))));
				americoldObj.setCreated_At(((results.getDate("Created_At"))));
				americoldList.add(americoldObj);
			}
			if(americoldList.size()>0) {
				ObjectMapper mapper = new ObjectMapper();
				 JsonOutput = mapper.writeValueAsString(americoldList);
			}
		
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		}

		return JsonOutput;
	}

	public Object handleRequest(Object input, Context context) {
		System.out.println("Americold Order Processing Start **************");
		System.out.println("input::"+input);
		System.out.println("context::"+context);
		Object data=loadOrderData();
		System.out.println("Americold Order Processing End **************"+data);
        return data;
	}

}
