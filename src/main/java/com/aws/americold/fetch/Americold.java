/**
* Entity to hold americold object
*
* @author Naveen Kumar
*/
package com.aws.americold.fetch;

import java.io.Serializable;
import java.sql.Date;

public class Americold implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private int Order_Id; 
	
	private String Customer_Id; 
	
	private String Product_Id; 

	private String Product_Name; 

	private String Quantity; 


	private String Order_Status; 
	
	private Date Created_At;

	public int getOrder_Id() {
		return Order_Id;
	}

	public void setOrder_Id(int order_Id) {
		Order_Id = order_Id;
	}

	public String getCustomer_Id() {
		return Customer_Id;
	}

	public void setCustomer_Id(String customer_Id) {
		Customer_Id = customer_Id;
	}

	public String getProduct_Id() {
		return Product_Id;
	}

	public void setProduct_Id(String product_Id) {
		Product_Id = product_Id;
	}

	public String getProduct_Name() {
		return Product_Name;
	}

	public void setProduct_Name(String product_Name) {
		Product_Name = product_Name;
	}

	public String getQuantity() {
		return Quantity;
	}

	public void setQuantity(String quantity) {
		Quantity = quantity;
	}

	public String getOrder_Status() {
		return Order_Status;
	}

	public void setOrder_Status(String order_Status) {
		Order_Status = order_Status;
	}

	public Date getCreated_At() {
		return Created_At;
	}

	public void setCreated_At(Date created_At) {
		Created_At = created_At;
	} 

}
