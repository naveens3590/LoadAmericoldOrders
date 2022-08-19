package com.aws.americold.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class CommonService {
	/**
	 * This method creates a folder and add the content to the file 
	 * itself
	 */
	public static void createFolder(String bucketName, String folderName, AmazonS3 client,String SUFFIX) {
		// create meta-data for your folder and set content-length to 0
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(20);
		 String data = "Hello Americold Member";

		// create empty content+
		InputStream content = new ByteArrayInputStream(data.getBytes());
		//InputStream content = IOUtils.toInputStream("Hello Americold", StandardCharsets.UTF_8);
		// create a PutObjectRequest passing the folder name suffixed by /
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName + SUFFIX, content,
				metadata);

		// send request to S3 to create folder
		client.putObject(putObjectRequest);
	}

}
	

