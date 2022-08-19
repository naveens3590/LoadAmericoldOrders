package com.aws.americold;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.aws.americold.service.CommonService;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AmericoldHandler implements RequestHandler<Object, Object> {
	private static Statement stmt = null;
	private static ResultSet results;

	public static List<String> getData() {
		List<String> americoldStringLst = new ArrayList<String>();

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
			for (Americold itrElem : americoldList) {
				String sql_update = "Update americold set Order_Status='InProgress' where Order_Id=?";
				sql_update = sql_update.replace("?", "" + itrElem.getOrder_Id());
				stmt = conn.createStatement();
				stmt.executeUpdate(sql_update);
				americoldStringLst.add("OrderId:" + itrElem.getOrder_Id() + " CustomerId:" + itrElem.getCustomer_Id()
						+ " ProductId:" + itrElem.getProduct_Id() + " ProductName:" + itrElem.getProduct_Name()
						+ " Quantity:" + itrElem.getQuantity() + " OrderStatus:" + itrElem.getOrder_Status()
						+ " Created_At:" + itrElem.getCreated_At());
			}
			conn.setAutoCommit(true);
			ObjectMapper mapper = new ObjectMapper();
			String JSONOutput = mapper.writeValueAsString(americoldList);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		}

		return americoldStringLst;

	}

	public Object handleRequest(Object input, Context context) {
		System.out.println("Americold Order Processing Start **************");
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date date = new Date();

		// credentials object identifying user for authentication
		// user must have AWSConnector and AmazonS3FullAccess for
		// this example to work
		AWSCredentials credentials = new BasicAWSCredentials(CommonConstants.ACCESS_KEY_ID,
				CommonConstants.ACCESS_SEC_KEY);

		// create a client connection based on credentials
		// AmazonS3 s3client = new AmazonS3Client(credentials);

		AmazonS3 s3client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_2).build();

		// create bucket - name must be unique for all S3 users
		String bucketName = CommonConstants.BUCKET_NAME;
		// s3client.createBucket(bucketName);

		// create folder into bucket
		String folderName = CommonConstants.FOLDER_NAME;
		Path tempFile = null;
		try {
			tempFile = Files.createTempFile("americold" + date, null);
			List<String> content = getData();
			Files.write(tempFile, content, StandardOpenOption.APPEND);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		CommonService.createFolder(bucketName, folderName, s3client, CommonConstants.SUFFIX + tempFile.getFileName());

		String fileName = folderName + CommonConstants.SUFFIX + tempFile.getFileName().toString();
		s3client.putObject(new PutObjectRequest(bucketName, fileName, tempFile.toFile())
				.withCannedAcl(CannedAccessControlList.PublicRead));


		// commonService.getObj(s3client);

		// CommonService.deleteFolder(bucketName, folderName, s3client);

		// deletes bucket
		System.out.println("Americold Order Processing End **************");
		return "Execution of Americold Job Done :" + formatter.format(date);
	}

	public static void main(String[] args) {
		System.out.println("Americold Order Processing Start **************");
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date date = new Date();

		// credentials object identifying user for authentication
		// user must have AWSConnector and AmazonS3FullAccess for
		// this example to work
		AWSCredentials credentials = new BasicAWSCredentials(CommonConstants.ACCESS_KEY_ID,
				CommonConstants.ACCESS_SEC_KEY);

		// create a client connection based on credentials
		// AmazonS3 s3client = new AmazonS3Client(credentials);

		AmazonS3 s3client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_2).build();

		// create bucket - name must be unique for all S3 users
		String bucketName = CommonConstants.BUCKET_NAME;
		// s3client.createBucket(bucketName);

		// create folder into bucket
		String folderName = CommonConstants.FOLDER_NAME;
		Path tempFile = null;
		try {
			tempFile = Files.createTempFile("americold" + date.getDate(), null);
			List<String> content = getData();
			Files.write(tempFile, content, StandardOpenOption.APPEND);
		} catch (IOException e) {
			e.printStackTrace();
		}
		CommonService.createFolder(bucketName, folderName, s3client, CommonConstants.SUFFIX + tempFile.getFileName());

		String fileName = folderName + CommonConstants.SUFFIX + tempFile.getFileName().toString();
		s3client.putObject(new PutObjectRequest(bucketName, fileName, tempFile.toFile())
				.withCannedAcl(CannedAccessControlList.PublicRead));

		System.out.println("Americold Order Processing End **************");
		// commonService.getObj(s3client);

		// CommonService.deleteFolder(bucketName, folderName, s3client);
		System.out.println("Execution of Americold Job Done :" + formatter.format(date));
		// deletes bucket
	}
}
