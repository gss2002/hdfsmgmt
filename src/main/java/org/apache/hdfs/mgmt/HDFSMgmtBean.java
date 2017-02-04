package org.apache.hdfs.mgmt;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSMgmtBean {
	private static final Logger LOG = LoggerFactory.getLogger(HDFSMgmtBean.class);

	static boolean userFolder = false;
	public static String ldapGroup = "hdpdev-user";
	public static String gcbaseDn = "dc=hdpusr,dc=senia,dc=org";
	public static String gcldapURL = "ldap://seniadc1.hdpusr.senia.org:3268";
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
