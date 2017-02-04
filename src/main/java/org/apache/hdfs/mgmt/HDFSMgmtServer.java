package org.apache.hdfs.mgmt;

import java.io.File;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSMgmtServer implements Daemon {
	private static final Logger LOG = LoggerFactory.getLogger(HDFSMgmtServer.class);
	static Options options = new Options();
	static Configuration conf = new Configuration();
    UsrFolderThread csuf;



	
	public static void setHdpConfig(){
		HDFSMgmtBean.hdpConfig.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		HDFSMgmtBean.hdpConfig.addResource(new Path("/etc/hive/conf/hive-site.xml"));
		HDFSMgmtBean.hdpConfig.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		HDFSMgmtBean.hdpConfig.set("hadoop.security.authentication", "kerberos");
		UserGroupInformation.setConfiguration(HDFSMgmtBean.hdpConfig);
		LOG.info("Config: " + HDFSMgmtBean.hdpConfig.get("hadoop.security.authentication"));
		LOG.info("Config: " + HDFSMgmtBean.hdpConfig.get("dfs.namenode.kerberos.principal"));
		LOG.info("Config: " + HDFSMgmtBean.hdpConfig.get("fs.defaultFS"));
	}

		
	private static void missingParams() {
		String header = "HDFS User Folder Management and /tmp file system Cleanup";
		String footer = "\nPlease report issues at http://github.com/gss2002";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("get", header, options, footer, true);
		System.exit(0);
	}

	@Override
	public void init(DaemonContext context) throws DaemonInitException, Exception {
		// TODO Auto-generated method stub
		
		HDFSMgmtBean.init();
		HDFSMgmtBean.daemon = true;
		LOG.debug("Daemon initialized with arguments {}.", context.getArguments().toString());

		Configuration conf = new Configuration();
		String[] otherArgs = null;
		try {
			otherArgs = new GenericOptionsParser(conf, context.getArguments()).getRemainingArgs();
		} catch (IOException e4) {
			// TODO Auto-generated catch block
			LOG.error(e4.getMessage());
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
			LOG.error(e2.getMessage());
		}
		if (cmd.hasOption("userfolder") || cmd.hasOption("tmpcleanup")) {
			if (cmd.hasOption("userfolder")) {
				HDFSMgmtBean.ldapGroup = cmd.getOptionValue("ldapGroup");
				HDFSMgmtBean.userFolder = true;
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
	    	HDFSMgmtBean.hdfs_keytab = cmd.getOptionValue("hdfs_keytab");
	    	HDFSMgmtBean.hdfs_keytabupn = cmd.getOptionValue("hdfs_upn");
	    	File hdfs_keytabFile = new File(HDFSMgmtBean.hdfs_keytab);
	    	if (hdfs_keytabFile.exists()) {
	    		if (!(hdfs_keytabFile.canRead())) { 
		    		LOG.error("HDFS KeyTab  exists but cannot read it - exiting");
		    		System.exit(1);
	    		}
	    		HDFSMgmtBean.useHdfsKeytab=true;
	    	} else {
	    		LOG.error("HDFS KeyTab doesn't exist  - exiting");
	    		System.exit(1);
	    	}
	    }
	    if (cmd.hasOption("ad_keytab") && cmd.hasOption("ad_upn")) {
	    	HDFSMgmtBean.ad_keytab = cmd.getOptionValue("ad_keytab");
	    	HDFSMgmtBean.ad_keytabupn = cmd.getOptionValue("ad_upn");
	    	File ad_keytabFile = new File(HDFSMgmtBean.ad_keytab);
	    	if (ad_keytabFile.exists()) {
	    		if (!(ad_keytabFile.canRead())) { 
		    		LOG.error("AD KeyTab  exists but cannot read it - exiting");
		    		System.exit(1);
	    		}
	    		HDFSMgmtBean.useAdKeytab=true;
	    	} else {
	    		LOG.error("AD KeyTab doesn't exist  - exiting");
	    		System.exit(1);
	    	}
	    }
	    setHdpConfig();


	}

	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub
	    csuf = new UsrFolderThread();
	    csuf.setName("CreateSetUsrFolder-Thread");
	    csuf.setDaemon(true);
	    csuf.start();
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
		csuf.interrupt();
		
		
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		csuf.interrupt();
	}

}
