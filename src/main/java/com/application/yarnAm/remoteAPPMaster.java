package com.application.yarnAm;

import com.application.MySeriaData.ByteTrans;
import com.application.MySeriaData.ConfigureAPI;
import com.application.MySeriaData.MyAllocateRequest;
import com.application.MySeriaData.MyAllocateResponse;
import com.application.UmAm.UnmanagedAMLauncher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import com.application.api.AMRMClient;
import com.application.api.NMClient;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class remoteAPPMaster implements AmAllocateService{

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

	public remoteAPPMaster(){
		LOG.info("new a am and a hadoop rpc server!");
	}
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
		clusterMake cM = new clusterMake(conf);
		Thread t1 = new Thread(cM);
		LOG.info("in remoteAPPMaster thread start!");
		t1.start();
		LOG.info("in remoteAPPMaster thread start finish!");


	}

	/*public AllocateResponse GetAlloRequest(AllocateRequest request)
			throws YarnException, IOException, InterruptedException {
		//modify the AMRMClient to accept the request
		//use function addRequestAllocate
		LOG.info("in remoteAPPMaster GetAlloRequest1!");

		requestTmp = request;
		isNewRequest = true;


		while(canReturnRep == false){
			isNewRequest = true;
			Thread.sleep(1000);
			LOG.info("in remoteAPPMaster GetAlloRequest,wait for am to get response! isNewRequest: "+isNewRequest);

		}
		LOG.info("in remoteAPPMaster GetAlloRequest2!");

		canReturnRep = false;
		return allRep;
	}
*/
	public boolean setUnRegister(){
		IsUnRegister = true;
		return true;
	}

	//////////////////////////////////////////////////////////////////////////
	// ///////////////////Am and hadoop rpc

	public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2) throws IOException {
		return this.getProtocolSignature(arg0, arg1, arg2);
	}
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return ConfigureAPI.VersionID.RPC_VERSION;
	}

	public IntWritable add(IntWritable arg1, IntWritable arg2) {
		return new IntWritable(arg1.get() + arg2.get());
	}

	public IntWritable sub(IntWritable arg1, IntWritable arg2) {
		return new IntWritable(arg1.get() - arg2.get());
	}

	public BytesWritable tansBytes(BytesWritable args) throws Exception {
		LOG.info("in tansBytes! tans back to MyAllocateRequest ");
		MyAllocateRequest myAllocateRequest = (MyAllocateRequest) ByteTrans.bytesToObject(args.getBytes());
		if(myAllocateRequest == null)
			LOG.error("in tansBytes! null myAllocateRequest pointer! ");

		LOG.info("in tansBytes! tans  to allocateRequest ");
		AllocateRequest allocateRequest = AllocateRequest.newInstance(
				myAllocateRequest.getResponseId(),
				myAllocateRequest.getProgress(),
				myAllocateRequest.getAskList(),
				myAllocateRequest.getReleaseList(),
				myAllocateRequest.getResourceBlacklistRequest());
		requestTmp = allocateRequest;
		isNewRequest = true;


		while(canReturnRep == false){
			isNewRequest = true;
			Thread.sleep(1000);
			LOG.info("in remoteAPPMaster tansBytes,wait for am to get response! isNewRequest: "+isNewRequest);

		}
		LOG.info("inremoteAPPMaster tansBytes !");

		canReturnRep = false;
		/*
		(int responseId,
                                               List<ContainerStatus> completedContainers,
                                               List<Container> allocatedContainers, List<NodeReport> updatedNodes,
                                               Resource availResources, AMCommand command, int numClusterNodes,
                                               PreemptionMessage preempt, List<NMToken> nmTokens)
		 */
		MyAllocateResponse myAllocateResponse = MyAllocateResponse.newInstance(
				allRep.getResponseId(),
				allRep.getCompletedContainersStatuses(),
				allRep.getAllocatedContainers(),
				allRep.getUpdatedNodes(),
				allRep.getAvailableResources(),
				allRep.getAMCommand(),
				allRep.getNumClusterNodes(),
				allRep.getPreemptionMessage(),
				allRep.getNMTokens());
		if(myAllocateResponse == null){
			LOG.error("myAllocateResponse is null!!");
			System.exit(-1);
		}

		return new BytesWritable(ByteTrans.ObjectToBytes(myAllocateResponse));
	}
	public static class RpcServerThread implements Runnable{
		public void run() {
			try {
				RPC.Server server = new RPC.Builder(new Configuration()).setProtocol(AmAllocateService.class)
						.setBindAddress("127.0.0.1").setPort(IPC_PORT).setInstance(new remoteAPPMaster()).build();
				server.start();
				LOG.info("allocate server has started");
				System.in.read();
				LOG.info("allocate server has started2");

			} catch (Exception ex) {
				ex.printStackTrace();
				LOG.error("allocate server  error,message is " + ex.getMessage());
			}

		}
	}
	public static final int IPC_PORT = 9090;
	// provide main method so this class can act as AM
	public static void main(String[] args) throws Exception {
		System.out.println("!!!!!!!!!!!!!!!!!!!!!stop a while in my main!!!!!!!!!!!!!!!!!!!!!!!!!!");
		// Thread.sleep(200);
		if (args[0].equals("success")) {
			//start a hadoop rpc server!
			RpcServerThread rpcServerThread = new RpcServerThread();
			Thread rS = new Thread(rpcServerThread);
			LOG.info("in remoteAPPMaster main start rpc server thread!");
			rS.start();
			LOG.info("in remoteAPPMaster main start rpc server thread success!");

			final String command = "hello";
			// Initialize clients to ResourceManager and NodeManagers
			LOG.info("go to start the amrmclient in main to allocate resource");
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

			for (int i = 0; i < 2; ++i) {
				AMRMClient.ContainerRequest containerAsk =
						new AMRMClient.ContainerRequest(capability, null, null, priority);
				System.out.println("Making res-req " + i);
				rmClient.addContainerRequest(containerAsk);
			}

			while (!IsUnRegister) {
				//System.out.println("begin a container work !");
				LOG.info("in ams am main while1 !");
				LOG.info("isNewRequest: "+isNewRequest);
				if(isNewRequest){
					LOG.info("in ams am main while2 !");

					int i = 0;
					List<AllocateResponse> listRsp = new ArrayList<AllocateResponse>();
					int containerAskNum = 2;//requestTmp.getAskList().size();
					while(i<containerAskNum) {
						AllocateResponse response = rmClient.allocate(i);
						listRsp.add(response);
						i += response.getAllocatedContainers().size();
						LOG.info("in ams am main while3 response container!"+i);
						Thread.sleep(500);
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
			LOG.info("!!!!!!!!!!!!!!!!!!!!!stop a while in useUMALauncher!!!!!!!!!!!!!!!!!!!!!!!!!!");

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
		List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
		List<NodeReport> allUpdateNode = new ArrayList<NodeReport>();
		List<Container> allAllocatedContainer = new ArrayList<Container>();
		int allMen = 0;
		int allVCore = 0;
		int allCN = 0;
		List<NMToken> allNMTokens = new ArrayList<NMToken>();
		List<ContainerResourceIncrease> allCRI = new ArrayList<ContainerResourceIncrease>();
		List<ContainerResourceDecrease> allCRD = new ArrayList<ContainerResourceDecrease>();

		for(Iterator it2 = allReList.iterator(); it2.hasNext();){
			AllocateResponse allocateResponse = (AllocateResponse)it2.next();
			completedContainers.addAll(allocateResponse.getCompletedContainersStatuses());
			allUpdateNode.addAll(allocateResponse.getUpdatedNodes());
			allAllocatedContainer.addAll(allocateResponse.getAllocatedContainers());
			allMen += allocateResponse.getAvailableResources().getMemory();
			allVCore += allocateResponse.getAvailableResources().getVirtualCores();
			allCN += allocateResponse.getNumClusterNodes();
			allNMTokens.addAll(allocateResponse.getNMTokens());
			allCRI.addAll(allocateResponse.getIncreasedContainers());
			allCRD.addAll(allocateResponse.getDecreasedContainers());

		}


		Resource allAvailResource = Resource.newInstance(allMen,allVCore);


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
