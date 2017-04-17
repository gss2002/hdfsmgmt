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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UsrFolderThread extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(UsrFolderThread.class);

	static InitialDirContext ctx = null;
	public static String gcbaseDn = "";
	public static String gcldapURL = "";
	static String ldapGroup = "hdpdev-user";
	static LdapClient gcldpClient = null;
	static LdapApi gcapi = null;
	static FileSystem fs = null;
	static String type = null;
	static boolean userFolder = false;
	static KerberosClient krbClient;
	private final Object lock = new Object();
	private final Object userProclock = new Object();
	static String userPrincipalName;

	UserGroupInformation ugi = null;

	public UsrFolderThread() {
		ldapGroup = HDFSMgmtBean.ldapGroup;
		gcbaseDn = HDFSMgmtBean.gcbaseDn;
		gcldapURL = HDFSMgmtBean.gcldapURL;
		try {
			userPrincipalName = UserGroupInformation.getCurrentUser().getUserName();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	@Override
	public void run() {
		if (HDFSMgmtBean.daemon) {
			synchronized (lock) {
				while (true) {
					try {
						manageFolders();
						lock.wait(300000L);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		} else {
			manageFolders();
		}
	}

	public void manageFolders() {

		try {
			if (HDFSMgmtBean.useHdfsKeytab) {
	            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(HDFSMgmtBean.hdfs_keytabupn,HDFSMgmtBean.hdfs_keytab);
	            UserGroupInformation.setLoginUser(ugi);
			} else {
				ugi = UserGroupInformation.getCurrentUser();
			}
			LOG.debug("HdfsUPN: "+ugi.getUserName());
			if (HDFSMgmtBean.useAdKeytab) {
				LOG.debug("adupn: "+HDFSMgmtBean.ad_keytabupn);
				krbClient = new KerberosClient(HDFSMgmtBean.ad_keytabupn, null, HDFSMgmtBean.ad_keytab);

			} else {
				LOG.debug("adupn: "+userPrincipalName);
				krbClient = new KerberosClient(userPrincipalName);				
			}
			gcldpClient = new LdapClientSASL(gcbaseDn, gcldapURL, krbClient.getSubject());
			gcapi = new LdapApi();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			LOG.error(e1.getMessage());
		}
		ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
		try {
			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				public Void run() throws Exception {

					try {
						fs = FileSystem.get(HDFSMgmtBean.hdpConfig);
					} catch (IOException e2) {
						// TODO Auto-generated catch block
						LOG.error(e2.getMessage());
					}
					if (HDFSMgmtBean.userFolder) {
						LOG.info("LdapGroup: " + ldapGroup);
						String groupSamAccountName = gcapi.getSamAccountNameFromCN(gcldpClient, gcbaseDn, ldapGroup);
						LOG.info("GroupSamAccountName: " + groupSamAccountName);
						Map<String, Attribute> groupResults = gcapi.getADGroupGCAttrs(gcldpClient, gcbaseDn,
								groupSamAccountName);
						LOG.debug("groupResults Null: " + groupResults.isEmpty());
						if (gcapi.groupRangingExists(groupResults)) {
							LOG.debug("getGroupMembers - Ranging=TRUE");
							List<String> groupMbrList = gcapi.getGroupMemberRanging(gcldpClient, gcbaseDn,
									groupSamAccountName);
							int counter = 0;
							LOG.debug("Checking LdapUsers: " + groupMbrList.size());
							for (int i = 0; i < groupMbrList.size(); i++) {
								String member = gcapi.getSamAccountName(
										gcapi.getUserDNAttrs(gcldpClient, gcbaseDn, groupMbrList.get(i)));
								if (HDFSMgmtBean.lcaseUid) {
									member = member.toLowerCase();
								}
								UsrFolderThreadImpl u = new UsrFolderThreadImpl(member, HDFSMgmtBean.hdpConfig, fs);
								u.start();
								counter++;
								synchronized (userProclock) {
									if (counter == 75) {
										userProclock.wait(10000L);
										counter = 0;
									}
								}
							}
						} else {
							LOG.debug("getGroupMembers - Ranging=FALSE");
							List<String> groupMbrList = gcapi.getGroupMembers(groupResults);
							if (groupMbrList != null) {
								int counter = 0;
								LOG.debug("Checking LdapUsers: " + groupMbrList.size());
								for (int i = 0; i < groupMbrList.size(); i++) {
									String member = gcapi.getSamAccountName(
											gcapi.getUserDNAttrs(gcldpClient, gcbaseDn, groupMbrList.get(i)));
									if (HDFSMgmtBean.lcaseUid) {
										member = member.toLowerCase();
									}
									UsrFolderThreadImpl u = new UsrFolderThreadImpl(member, HDFSMgmtBean.hdpConfig, fs);
									u.start();
									counter++;
									synchronized (userProclock) {
										if (counter == 75) {
											userProclock.wait(10000L);
											counter = 0;
										}
									}
								}
							}

						}

						LdapClient.destroyLdapClient(gcldpClient.getLdapBean().getLdapCtx());

					}
					return null;
				}
			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		}

	}

}
