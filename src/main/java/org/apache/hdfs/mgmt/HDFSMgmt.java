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

import java.io.File;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSMgmt {
	private static final Logger LOG = LoggerFactory.getLogger(HDFSMgmt.class);
	static Options options = new Options();




	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HDFSMgmtBean.init();
		HDFSMgmtBean.daemon = false;
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
		options.addOption("ldapServer", true, "LdapServer for usage with --userfolder option");
		options.addOption("ldapSSL", false, "LdapSSL for usage with --userfolder option");
		options.addOption("ldapBaseDN", true, "LDAPBaseDN sets LDAPBaseDN --userfolder option");
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
				HDFSMgmtBean.ldapGroup = cmd.getOptionValue("ldapGroup");
				HDFSMgmtBean.userFolder = true;
				if (!cmd.hasOption("ldapGroup")) {
					missingParams();
				}
				if (cmd.hasOption("ldapSSL")) {
					System.setProperty("ldap.ssl", "true");
				} else {
					System.setProperty("ldap.ssl", "false");
				}
				if (!(cmd.hasOption("ldapServer"))) {
					missingParams();
				} else {
					System.setProperty("ldapServer", cmd.getOptionValue("ldapServer"));
				}
				if (!(cmd.hasOption("ldapBaseDN"))) {
					missingParams();
				} else {
					System.setProperty("ldapBaseDN", cmd.getOptionValue("ldapBaseDN"));
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
	    UsrFolderThread csuf = new UsrFolderThread();
	    csuf.setName("CreateSetUsrFolder-Thread");
	    csuf.start();

	}
	
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

}
