package org.apache.hdfs.mgmt;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class UsrFolderThreadImpl extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(UsrFolderThreadImpl.class);

	Configuration hdpConfig;
	String ldapUsr;
	FileSystem fs;

	
	UsrFolderThreadImpl(String ldapUser, Configuration hdpConfigIn, FileSystem fs) {
		this.hdpConfig=hdpConfigIn;
		this.ldapUsr=ldapUser;
		this.fs=fs;
		
	}
	@Override
	public void run() {
		userFolder(this.ldapUsr, this.hdpConfig, this.fs);
		// TODO Auto-generated method stub

	
        
	}
	public void userFolder(String ldapUser, Configuration hdpConfig, FileSystem fs) {
		try {
			createUserFolder(ldapUser, hdpConfig, fs);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		}
		try {
			setUserPerm(ldapUser, hdpConfig, fs);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		}
	}

	public void setUserPerm(String user, Configuration hdpConfig, FileSystem fs) throws IOException {
	      FsShell shell=new FsShell(hdpConfig);
	      try {
	    	  shell.run(new String[]{"-chmod","-R","700","/user/"+user+""});
	    	  shell.run(new String[]{"-chown","-R",""+user+"","/user/"+user+""});

	      }
	     catch (  Exception e) {
	    	 LOG.error("Couldnt change the file permissions for user "+user+" "+e.getStackTrace());
	        throw new IOException(e);
	     }
	}

	public void createUserFolder(String user, Configuration hdpConfig,FileSystem fs) throws IOException {
	      try {
	    	  if (!(fs.exists(new Path("/user/"+user)))) {
	    	      FsShell shell=new FsShell(hdpConfig);
	    		  shell.run(new String[]{"-mkdir","/user/"+user+""});
	    	  }

	      }
	     catch (  Exception e) {
	    	 LOG.error("Couldnt create user folder "+user+ " "+e.getStackTrace());
	        throw new IOException(e);
	     }
	}
}
