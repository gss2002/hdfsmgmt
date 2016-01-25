package org.apache.hdfs.mgmt;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import javax.naming.directory.Attribute;
import javax.naming.directory.InitialDirContext;

import org.apache.adldap.KerberosClient;
import org.apache.adldap.LdapApi;
import org.apache.adldap.LdapClient;
import org.apache.adldap.LdapClientSASL;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

public class HDFSMgmt {
	static Logger LOG = Logger.getLogger(HDFSMgmt.class);
	static boolean userFolder = false;
	static Options options = new Options();
	public static Configuration hdpConfig = new Configuration();
	public static String ldapGroup = "hdpdev-user";
	public static String gcbaseDn = "dc=hdpusr,dc=senia,dc=org";
	public static String gcldapURL = "ldap://seniadc.senia.org:3268";
	public static String hdfs_keytab = null;
	public static String hdfs_keytabupn = null;	
	public static boolean useHdfsKeytab = false;
	public static String ad_keytab = null;
	public static String ad_keytabupn = null;	
	public static boolean useAdKeytab = false;



	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		String[] otherArgs = null;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		} catch (IOException e4) {
			// TODO Auto-generated catch block
			e4.printStackTrace();
		}

		options.addOption("userfolder", false, "Create and/or set HDFS /user/username perms");
		options.addOption("tmpcleanup", false, "Clean up HDFS /tmp folder");
		options.addOption("ldapGroup", true, "LdapGroup for usage with --userfolder option");
	    options.addOption("hdfs_keytab", true, "HDFS SuperUser Kerberos keytab file to connect to HDFS --hdfs_keytab /etc/security/keytabs/hdfs.headless.keytab");
	    options.addOption("hdfs_upn", true, "HDFS SuperUser Kerberos Princpial for keytab to connect to HDFS --hdfs_upn hdfs-tech@TECH.HDP.EXAMPLE.COM");
	    options.addOption("ad_keytab", true, "AD Ldap Keytab File to connect to LDAP for Group Membership --ad_keytab $HOME/aduser.keytab");
	    options.addOption("ad_upn", true, "AD Ldap Kerberos Princpial for Keytab to connect to LDAP for Group Membership --ad_upn username@NT.EXAMPLE.COM");
	    options.addOption("help", false, "Display help");
		CommandLineParser parser = new HDFSMgmtParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, otherArgs);
		} catch (ParseException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		if (cmd.hasOption("userfolder") || cmd.hasOption("tmpcleanup")) {
			if (cmd.hasOption("userfolder")) {
				ldapGroup = cmd.getOptionValue("ldapGroup");
				userFolder = true;
				if (!cmd.hasOption("ldapGroup")) {
					missingParams();
				}
			}
			if (cmd.hasOption("tmpcleanup")){
		
			}
		} else {
			missingParams();
		}
	    if (cmd.hasOption("hdfs_keytab") && cmd.hasOption("hdfs_upn")) {
	    	hdfs_keytab = cmd.getOptionValue("hdfs_keytab");
	    	hdfs_keytabupn = cmd.getOptionValue("hdfs_upn");
	    	File hdfs_keytabFile = new File(hdfs_keytab);
	    	if (hdfs_keytabFile.exists()) {
	    		if (!(hdfs_keytabFile.canRead())) { 
		    		System.out.println("HDFS KeyTab  exists but cannot read it - exiting");
		    		System.exit(1);
	    		}
		    	useHdfsKeytab=true;
	    	} else {
	    		System.out.println("HDFS KeyTab doesn't exist  - exiting");
	    		System.exit(1);
	    	}
	    }
	    if (cmd.hasOption("ad_keytab") && cmd.hasOption("ad_upn")) {
	    	ad_keytab = cmd.getOptionValue("ad_keytab");
	    	ad_keytabupn = cmd.getOptionValue("ad_upn");
	    	File ad_keytabFile = new File(ad_keytab);
	    	if (ad_keytabFile.exists()) {
	    		if (!(ad_keytabFile.canRead())) { 
		    		System.out.println("AD KeyTab  exists but cannot read it - exiting");
		    		System.exit(1);
	    		}
		    	useAdKeytab=true;
	    	} else {
	    		System.out.println("AD KeyTab doesn't exist  - exiting");
	    		System.exit(1);
	    	}
	    }
	    setHdpConfig();
	    UsrFolderThread csuf = new UsrFolderThread();
	    csuf.setName("CreateSetUsrFolder-Thread");
	    csuf.start();

	}
	
	public static void setHdpConfig(){
		hdpConfig.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		hdpConfig.addResource(new Path("/etc/hive/conf/hive-site.xml"));
		hdpConfig.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		hdpConfig.set("hadoop.security.authentication", "kerberos");
		UserGroupInformation.setConfiguration(hdpConfig);
		System.out.println("Config: " + HDFSMgmt.hdpConfig.get("hadoop.security.authentication"));
		System.out.println("Config: " + HDFSMgmt.hdpConfig.get("dfs.namenode.kerberos.principal"));
		System.out.println("Config: " + HDFSMgmt.hdpConfig.get("fs.defaultFS"));
	}

		
	private static void missingParams() {
		String header = "HDFS User Folder Management and /tmp file system Cleanup";
		String footer = "\nPlease report issues at http://github.com/gss2002";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("get", header, options, footer, true);
		System.exit(0);
	}

}
