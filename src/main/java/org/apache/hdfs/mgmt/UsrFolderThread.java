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

public class UsrFolderThread extends Thread {
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
		ldapGroup = HDFSMgmt.ldapGroup;
		gcbaseDn = HDFSMgmt.gcbaseDn;
		gcldapURL = HDFSMgmt.gcldapURL;
		try {
			userPrincipalName = UserGroupInformation.getCurrentUser().getUserName();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	@Override
	public void run() {
		synchronized (lock) {
			while (true) {
				try {
					// manageFolders(this.ldapUsr, this.hdpConfig, this.fs);
					manageFolders();
					lock.wait(300000L);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public void manageFolders() {

		try {
			if (HDFSMgmt.useHdfsKeytab) {
	            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(HDFSMgmt.hdfs_keytabupn,HDFSMgmt.hdfs_keytab);
	            UserGroupInformation.setLoginUser(ugi);
			} else {
				ugi = UserGroupInformation.getCurrentUser();
			}
			System.out.println("HdfsUPN: "+ugi.getUserName());
			if (HDFSMgmt.useAdKeytab) {
				System.out.println("adupn: "+HDFSMgmt.ad_keytabupn);
				krbClient = new KerberosClient(HDFSMgmt.ad_keytabupn, null, HDFSMgmt.ad_keytab);

			} else {
				System.out.println("adupn: "+userPrincipalName);
				krbClient = new KerberosClient(userPrincipalName);				
			}
			gcldpClient = new LdapClientSASL(gcbaseDn, gcldapURL, krbClient.getSubject());
			gcapi = new LdapApi();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
		try {
			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				public Void run() throws Exception {

					try {
						fs = FileSystem.get(HDFSMgmt.hdpConfig);
					} catch (IOException e2) {
						// TODO Auto-generated catch block
						e2.printStackTrace();
					}
					if (HDFSMgmt.userFolder) {
						System.out.println("LdapGroup: " + ldapGroup);
						String groupSamAccountName = gcapi.getSamAccountNameFromCN(gcldpClient, gcbaseDn, ldapGroup);
						System.out.println("GroupSamAccountName: " + groupSamAccountName);
						Map<String, Attribute> groupResults = gcapi.getADGroupGCAttrs(gcldpClient, gcbaseDn,
								groupSamAccountName);
						System.out.println("groupResults Null: " + groupResults.isEmpty());
						if (gcapi.groupRangingExists(groupResults)) {
							System.out.println("getGroupMembers - Ranging=TRUE");
							List<String> groupMbrList = gcapi.getGroupMemberRanging(gcldpClient, gcbaseDn,
									groupSamAccountName);
							int counter = 0;
							System.out.println("Checking LdapUsers: " + groupMbrList.size());
							for (int i = 0; i < groupMbrList.size(); i++) {
								String member = gcapi.getSamAccountName(
										gcapi.getUserDNAttrs(gcldpClient, gcbaseDn, groupMbrList.get(i)));
								UsrFolderThreadImpl u = new UsrFolderThreadImpl(member, HDFSMgmt.hdpConfig, fs);
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
							System.out.println("getGroupMembers - Ranging=FALSE");
							List<String> groupMbrList = gcapi.getGroupMembers(groupResults);
							if (groupMbrList != null) {
								int counter = 0;
								System.out.println("Checking LdapUsers: " + groupMbrList.size());
								for (int i = 0; i < groupMbrList.size(); i++) {
									String member = gcapi.getSamAccountName(
											gcapi.getUserDNAttrs(gcldpClient, gcbaseDn, groupMbrList.get(i)));
									UsrFolderThreadImpl u = new UsrFolderThreadImpl(member, HDFSMgmt.hdpConfig, fs);
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
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
