package org.apache.hdfs.mgmt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsThreadHealth extends Thread {
	UsrFolderThread csuf;
	private final Object lock = new Object();
	private boolean stopThreads = true;
	private static final Logger LOG = LoggerFactory.getLogger(HdfsThreadHealth.class);


	public HdfsThreadHealth() {
	}

	
	@Override
	public void run() {
		while (stopThreads) {
			if (csuf == null) {
				LOG.info("CreateSetUsrFolder-Thread is null");
			    csuf = new UsrFolderThread();
			}
			if (!(csuf.isAlive())) {
				LOG.info("CreateSetUsrFolder-Thread: "+csuf.isAlive());
			    csuf = new UsrFolderThread();
				LOG.info("Creating or Re-Creating CreateSetUsrFolder-Thread");
			    csuf.setName("CreateSetUsrFolder-Thread");
			    csuf.setDaemon(true);
			    csuf.start();
				LOG.info("CreateSetUsrFolder-Thread: "+csuf.isAlive());
			}
			synchronized (lock) {
				try {
						lock.wait(300000L);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
	}
	@Override
	public void destroy(){
		LOG.info("CreateSetUsrFolder-Thread: "+csuf.isAlive()+" stopping");
		stopThreads = false;
		csuf.interrupt();
	}
	@Override
	public void interrupt(){
		LOG.info("CreateSetUsrFolder-Thread: "+csuf.isAlive()+" stopping");
		stopThreads = false;
		csuf.interrupt();
	}
}
