package com.application.myApp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import com.application.api.AMRMClient;
import com.application.api.NMClient;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class remoteAPPMaster {

	private final String  yarnAddressLabel = "yarn.resourcemanager.hostname";
	private static final Log LOG = LogFactory.getLog(remoteAPPMaster.class);
	//private AMRMClient<AMRMClient.ContainerRequest> rmClient;// client from am to rm

	protected static Configuration conf = new YarnConfiguration();

	protected static boolean IsUnRegister = false;

	protected static boolean canReturnRep = false;
	protected  static AllocateResponse allRep;
	protected static boolean isNewRequest = false;
	protected  static AllocateRequest requestTmp;
	private static String remoteYarnLabel;
	private static int amHostPort;


	public remoteAPPMaster(String remoteYarnLabel, int amHostPort) throws Exception {
		LOG.info("in remoteAPPMaster new!");

		this.remoteYarnLabel = remoteYarnLabel;
		this.amHostPort = amHostPort;
		conf = new YarnConfiguration();
		//resource manager address example 114.212.91.12
		String yarnString = conf.get(remoteYarnLabel);
		//modify the default yarn address to new one
		requestTmp = null;
		conf.set(yarnAddressLabel,"0.0.0.0");

	}

	public AllocateResponse GetAlloRequest(AllocateRequest request)
			throws YarnException, IOException, InterruptedException {
		//modify the AMRMClient to accept the request
		//use function addRequestAllocate
		LOG.info("in remoteAPPMaster GetAlloRequest1!");

		requestTmp = request;
		isNewRequest = true;

		clusterMake cM = new clusterMake(conf);
		Thread t1 = new Thread(cM);
		LOG.info("in remoteAPPMaster thread start!");
		t1.start();
		LOG.info("in remoteAPPMaster thread start finish!");

		while(canReturnRep == false){
			isNewRequest = true;
			Thread.sleep(1000);
			LOG.info("in remoteAPPMaster GetAlloRequest,wait for am to get response! isNewRequest: "+isNewRequest);

		}
		LOG.info("in remoteAPPMaster GetAlloRequest2!");

		canReturnRep = false;
		return allRep;
	}

	public boolean setUnRegister(){
		IsUnRegister = true;
		return true;
	}

	// provide main method so this class can act as AM
	public static void main(String[] args) throws Exception {
		System.out.println("!!!!!!!!!!!!!!!!!!!!!stop a while in main!!!!!!!!!!!!!!!!!!!!!!!!!!");
		// Thread.sleep(200);
		if (args[0].equals("success")) {

			final String command = "hello";
			// Initialize clients to ResourceManager and NodeManagers

			//conf.set("yarn.resourcemanager.hostname","114.212.87.91");
			AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
			conf.set("yarn.resourcemanager.address","0.0.0.0:8032");
			conf.set("yarn.resourcemanager.scheduler.address","0.0.0.0:8030");
			rmClient.init(conf);
			rmClient.start();
			// Register with ResourceManager
			LOG.info("registerApplicationMaster ...");
			rmClient.registerApplicationMaster("", 0, "");
			LOG.info("registerApplicationMaster success!");

			NMClient nmClient = NMClient.createNMClient();
			nmClient.init(conf);
			nmClient.start();


			// Priority for worker containers - priorities are intra-application
			Priority priority = Records.newRecord(Priority.class);
			priority.setPriority(0);

			// Resource requirements for worker containers
			Resource capability = Records.newRecord(Resource.class);
			capability.setMemory(128);
			capability.setVirtualCores(1);

			while (!IsUnRegister) {
				//System.out.println("begin a container work !");
				LOG.info("in ams am main while1 !");
				LOG.info("isNewRequest: "+isNewRequest);
				if(isNewRequest){
					LOG.info("in ams am main while2 !");

					int i = 0;
					List<AllocateResponse> listRsp = new ArrayList<AllocateResponse>();
					int containerAskNum = requestTmp.getAskList().size();
					while(i<containerAskNum) {
						AllocateResponse response = rmClient.allocate(requestTmp, 1);
						listRsp.add(response);
						i += response.getAllocatedContainers().size();
					}
					allRep = mergeResponseList(listRsp);
					isNewRequest = false;
					canReturnRep = true;
				}
				Thread.sleep(500);
			}
			LOG.info("Go to Finish am !");
			// Un-register with ResourceManager
			rmClient.unregisterApplicationMaster(
					FinalApplicationStatus.SUCCEEDED, "", "");
			LOG.info("Finish running am !");


		} else {
			System.exit(1);
		}
	}

	//an inner thread class for yarn client and application master
	private  static class clusterMake implements Runnable{


		private static final Log LOG = LogFactory
				.getLog(clusterMake.class);

		protected static Configuration myConf = new YarnConfiguration();
		public clusterMake(){
		}
		public clusterMake(Configuration conf){
			this.myConf = conf;
		}

		public void run() {

			try {
				this.useUMALauncher();
			} catch (Exception e) {
				e.printStackTrace();
			}
			LOG.info("finish thread in mySimpleAM");
		}



		public void useUMALauncher() throws Exception {
			LOG.info("!!!!!!!!!!!!!!!!!!!!!stop a while in testUMALauncher!!!!!!!!!!!!!!!!!!!!!!!!!!");

			String classpath = getTestRuntimeClasspath();
			LOG.info("classpath:" +classpath);

			String javaHome = System.getenv("JAVA_HOME");
			if (javaHome == null) {
				LOG.fatal("JAVA_HOME not defined. Test not running.");
				return;
			}

			String[] args = {
					"--classpath",
					classpath,
					"--queue",
					"default",
					"--cmd",
					javaHome
							+ "/bin/java -Xmx512m "
							+ remoteAPPMaster.class.getCanonicalName()
							+ " success" };

			LOG.info("Initializing Launcher");
			UnmanagedAMLauncher launcher =
					new UnmanagedAMLauncher();
			boolean initSuccess = launcher.init(args);
			LOG.info("Running Launcher");
			boolean result = launcher.run();

			LOG.info("Launcher run completed. Result=" + result);


		}

		private static int vetex = 0;

		public int getVetex(){
			return vetex;
		}



	}
	private static String getTestRuntimeClasspath() {
		LOG.info("Trying to generate classpath for app master from current thread's classpath");
		String envClassPath = "";
		String cp = System.getProperty("java.class.path");
		if (cp != null) {
			envClassPath += cp.trim() + File.pathSeparator;
		}
		// yarn-site.xml at this location contains proper config for mini cluster
		ClassLoader thisClassLoader = Thread.currentThread()
				.getContextClassLoader();
		java.net.URL url = thisClassLoader.getResource("yarn-site.xml");
		envClassPath += new File(url.getFile()).getParent();
		return envClassPath;
	}


	public static AllocateResponse mergeResponseList(List<AllocateResponse> responseList){

		//store all request
		List<AllocateResponse> allReList = new ArrayList<AllocateResponse>(responseList);

		//add complete container together, just add together
		List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
		for(Iterator it2 = allReList.iterator(); it2.hasNext();){
			List<ContainerStatus> Tmp = ((AllocateResponse)it2).getCompletedContainersStatuses();
			completedContainers.addAll((Tmp));
		}

		//nodeUpdate add
		List<NodeReport> allUpdateNode = new ArrayList<NodeReport>();
		for(Iterator it2 = allReList.iterator(); it2.hasNext();) {
			List<NodeReport> Tmp = ((AllocateResponse) it2).getUpdatedNodes();
			allUpdateNode.addAll(Tmp);
		}

		//allocatedContainers
		List<Container> allAllocatedContainer = new ArrayList<Container>();
		for(Iterator it2 = allReList.iterator(); it2.hasNext();) {
			List<Container> Tmp = ((AllocateResponse) it2).getAllocatedContainers();
			allAllocatedContainer.addAll(Tmp);
		}



		//add availResource
		int allMen = 0;
		int allVCore = 0;
		for(Iterator it2 = allReList.iterator(); it2.hasNext();) {
			allMen += ((AllocateResponse) it2).getAvailableResources().getMemory();
			allVCore += ((AllocateResponse) it2).getAvailableResources().getVirtualCores();
		}
		Resource allAvailResource = Resource.newInstance(allMen,allVCore);



		//add command, amcommond is a order to let the am to return to asyn(AM_RESYNC) or shutdown(AM_SHUTDOWN),
		// we need to manager all the amrm  client's stage!

		//add numClusterNodes,just add together
		int allCN = 0;
		for(Iterator it2 = allReList.iterator(); it2.hasNext();) {
			allCN += ((AllocateResponse) it2).getNumClusterNodes();
		}

		//add preempt, just return the local, competition

		//add nm nmTokens
		//nmTokens is the key to access to the  node management, we can just deal with it
		//for simply add together
		List<NMToken> allNMTokens = new ArrayList<NMToken>();
		for(Iterator it2 = allReList.iterator(); it2.hasNext();) {
			allNMTokens.addAll(((AllocateResponse)it2).getNMTokens());
		}

		//add increasedContainers(), just simply add them together
		List<ContainerResourceIncrease> allCRI = new ArrayList<ContainerResourceIncrease>();
		for(Iterator it2 = allReList.iterator(); it2.hasNext();) {
			allCRI.addAll(((AllocateResponse)it2).getIncreasedContainers());
		}

		//add decreasedContainer, just add them together
		List<ContainerResourceDecrease> allCRD = new ArrayList<ContainerResourceDecrease>();
		for(Iterator it2 = allReList.iterator(); it2.hasNext();) {
			allCRD.addAll(((AllocateResponse)it2).getDecreasedContainers());
		}




		AllocateResponse allResponse = AllocateResponse.newInstance(
				6,
				completedContainers,
				allAllocatedContainer,
				allUpdateNode,
				allAvailResource,
				allReList.get(0).getAMCommand(),
				allCN,
				allReList.get(0).getPreemptionMessage(),
				allNMTokens,
				allCRI,
				allCRD);

		//AllocateResponse.newInstance(responseId, completedContainers,
		//allocatedContainers, updatedNodes, availResources, command, numClusterNodes,
		//preempt, nmTokens, increasedContainers, decreasedContainers)
		return allResponse;
	}

}
