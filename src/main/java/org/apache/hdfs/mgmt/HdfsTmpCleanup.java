/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hdfs.mgmt;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HdfsTmpCleanup {
	private static final Logger LOG = LoggerFactory.getLogger(HdfsTmpCleanup.class);

	static Configuration hdpConfig = new Configuration();
	static FileSystem fs = null;
	static String type = null;
	static String url= null;
	static Path inPath = null;
	static long currentTime = 0L;
	static long deleteTime;
	static int days = 7;
	static String rootPath;
	static DateFormat formatter;
	static Calendar calendar;
	static LinkedList<Path> folderLinkedList= null;
	static FsShell shell;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	  	Configuration conf = new Configuration();
	    String[] otherArgs = null;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		} catch (IOException e4) {
			// TODO Auto-generated catch block
			LOG.error(e4.getMessage());
		}
		
	    currentTime = System.currentTimeMillis();
	    long daysMS = days * 86400000L;
	    LOG.debug("7Days in MS: "+daysMS);
	    deleteTime = currentTime-daysMS;
	    LOG.debug("DeleteTimefromCurrent MS: "+deleteTime);

		
	    hdpConfig.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
	    hdpConfig.addResource(new Path("/etc/hive/conf/hive-site.xml"));
	    hdpConfig.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
	    hdpConfig.set("hadoop.security.authentication", "kerberos");
		LOG.info("Config: "+hdpConfig.get("hadoop.security.authentication"));
		LOG.info("Config: "+hdpConfig.get("dfs.namenode.kerberos.principal"));
		LOG.info("Config: "+hdpConfig.get("fs.defaultFS"));
	    UserGroupInformation.setConfiguration(hdpConfig);
		UserGroupInformation ugi = null;
		inPath = new Path("/tmp");
		formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss.SSS");


	    calendar = Calendar.getInstance();
	    shell=new FsShell(hdpConfig);

		

		
		try {
			ugi = UserGroupInformation.getCurrentUser();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			LOG.error(e1.getMessage());
		}
	    ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
	    try {
			ugi.doAs(new PrivilegedExceptionAction<Void>() {

			    public Void run() throws Exception {

			    	try {
						fs = FileSystem.get(hdpConfig);
					} catch (IOException e2) {
						// TODO Auto-generated catch block
						LOG.error(e2.getMessage());
					}
			    	getFolderData(fs, inPath, 10);
	
					return null;
			    }
			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		}
		
		
	}
	
    public static void getFolderData(FileSystem fs, Path inPath, int depth) {
 		FileStatus[] fsList = null;
		try {
			fsList = fs.listStatus(inPath);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			LOG.error(e1.getMessage());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			LOG.error(e1.getMessage());
		}
		/*
		if (inPath.getName().toString().startsWith("temp")){ 
			getData(fs, inPath);
		} 
		if (inPath.getName().toString().contains("-hive_hive_")){
			getData(fs, inPath);
		}
		if (inPath.depth() >= 4) {
			getData(fs, inPath);

		}
		*/
		if (inPath.depth() == 2) {
			LOG.debug("Iterating: "+inPath);
 			if (folderLinkedList == null) {
 				folderLinkedList = new LinkedList<Path>();
 			}
			if (folderLinkedList.size() > 0 ){
		        Iterator<Path> itr = folderLinkedList.descendingIterator();
		        while(itr.hasNext()){
		            Path outPath = itr.next();
					try {


						long modifyTime = fs.getFileStatus(outPath).getModificationTime();

					    calendar.setTimeInMillis(modifyTime);
					    LOG.debug(modifyTime + " = " + formatter.format(calendar.getTime())+" Deleting Path::"+outPath);
		    			 try {
							shell.run(new String[]{"-rm","-R",""+outPath.toString().replace(fs.getUri().toString(), "")+""});
						} catch (Exception e) {
							// TODO Auto-generated catch block
							LOG.error(e.getMessage());
						}




					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

		            
		        }
				folderLinkedList.clear();
			}
		}
		getData(fs, inPath);


		if( depth != 0 ) {
			
			for(int i = 0; fsList != null && i < fsList.length; i++ ) {
				if (fsList[i].isDirectory()){
					inPath=fsList[i].getPath();
					getFolderData(fs,inPath, depth-1 );

				}
			}

		}
		
    }
    public static void getData(FileSystem fs, Path inPath) {
			try {
				String owner = fs.getFileStatus(inPath).getOwner();
				int depth = inPath.depth();
				long modifyTime = fs.getFileStatus(inPath).getModificationTime();

				if (modifyTime <= deleteTime && (!(owner.equalsIgnoreCase("hdfs")) && !(owner.equalsIgnoreCase("hive")) && !(owner.equalsIgnoreCase("oozie"))) ) {
						if (inPath.toString().contains("-hive/") && !(inPath.toString().contains("_tez_session_dir")) && depth > 3) {
							folderLinkedList.add(inPath);

						} 
						if (inPath.toString().contains("_tez_session_dir") && depth > 4){
							folderLinkedList.add(inPath);
						}
							
						if (inPath.toString().contains("/temp") && depth > 1) {
							folderLinkedList.add(inPath);
						}
						if (inPath.toString().contains("/sasdata_") && depth > 1) {
							folderLinkedList.add(inPath);
						}
						if (inPath.toString().contains("-hive_hive_") && depth > 1) {
							folderLinkedList.add(inPath);
						}
						
						LOG.debug("Delete Path="+inPath+" :: Depth="+inPath.depth()+" :: ModifyTime="+modifyTime+" :: Parent="+inPath.getParent().toString());



				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				LOG.error(e.getMessage());
			}
			
    }
	
	
}
