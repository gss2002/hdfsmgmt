package org.apache.hdfs.mgmt;

import org.apache.hadoop.conf.Configuration;

public class HDFSMgmtBean {
	static boolean userFolder = false;
	public static String ldapGroup = "hdpdev-user";
	public static String gcbaseDn = "dc=senia.org";
	public static String gcldapURL = "ldap://seniadc1.senia.org:3268";
	public static String hdfs_keytab = null;
	public static String hdfs_keytabupn = null;	
	public static boolean useHdfsKeytab = false;
	public static boolean daemon = false;
	public static String ad_keytab = null;
	public static String ad_keytabupn = null;	
	public static boolean useAdKeytab = false;
	public static boolean lcaseUid = true;
	public static Configuration hdpConfig;
	
	public HDFSMgmtBean() {
		 hdpConfig = new Configuration();
	}
	public static void init() {
		 hdpConfig = new Configuration();
	}
}
