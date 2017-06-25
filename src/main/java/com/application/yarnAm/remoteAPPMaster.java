package com.application.yarnAm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import com.application.MySeriaData.ByteTrans;
import com.application.MySeriaData.ConfigureAPI;
import com.application.MySeriaData.MyAllocateRequest;
import com.application.MySeriaData.MyAllocateResponse;
import com.application.UmAm.UnmanagedAMLauncher;
import com.application.api.AMRMClient;
import com.application.api.NMClient;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


public class remoteAPPMaster implements AmAllocateService{

	private final String  yarnAddressLabel = "yarn.resourcemanager.hostname";
	private static final Log LOG = LogFactory.getLog(remoteAPPMaster.class);
	//private AMRMClient<AMRMClient.ContainerRequest> rmClient;// client from am to rm

	protected static Configuration myConf = new YarnConfiguration();

	protected static boolean IsUnRegister = false;

	protected static boolean canReturnRep = false;
	protected  static AllocateResponse allRep;
	protected static boolean isNewRequest = false;
	protected  static AllocateRequest requestTmp;

	//////////////////////////////////////////////////////////////////////////
	// ///////////////////Am and hadoop rpc

	public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2) throws IOException {
		return this.getProtocolSignature(arg0, arg1, arg2);
	}
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return ConfigureAPI.VersionID.RPC_VERSION;
	}

	public remoteAPPMaster(){
		LOG.info("new a am and a hadoop rpc server!");
	}
	public remoteAPPMaster(Configuration conf, int amHostPort) throws Exception {
		LOG.info("in remoteAPPMaster new!");

		this.myConf = new YarnConfiguration(conf);

		requestTmp = null;
		clusterMake cM = new clusterMake(myConf,amHostPort);
		Thread t1 = new Thread(cM);
		LOG.info("in remoteAPPMaster thread start!");
		t1.start();
		LOG.info("in remoteAPPMaster thread start finish!");


	}

	public boolean setUnRegister(){
		IsUnRegister = true;
		return true;
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
		LOG.info(myAllocateRequest.getAskList().get(0).getCapability().getVirtualCores());
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
		private int amRPCServerHost = 9999;
		private final String localHost = "0.0.0.0";
		public RpcServerThread(int host){
			amRPCServerHost = host;
		}
		public void run() {
			try {
				RPC.Server server = new RPC.Builder(new Configuration()).setProtocol(AmAllocateService.class)
						.setBindAddress(localHost).setPort(amRPCServerHost).setInstance(new remoteAPPMaster()).build();
				server.start();
				LOG.info("allocate server has started");
				System.in.read();
				LOG.info("allocate server has started finish");
			} catch (Exception ex) {
				ex.printStackTrace();
				LOG.error("allocate server  error,message is " + ex.getMessage());
			}

		}
	}
	public static final int IPC_PORT = 9090;
	// provide main method so this class can act as AM
	private final static String yraLabel = "yarn.resourcemanager.address";
	private final static String yrsaLabel = "yarn.resourcemanager.scheduler.address";
	public static void main(String[] args) throws Exception {
		System.out.println("!!!!!!!!!!!!!!!!!!!!!stop a while in my main!!!!!!!!!!!!!!!!!!!!!!!!!!");
		// Thread.sleep(200);
		String[] temp = args[0].split(":");
		if (temp[0].equals("success")) {
			//start a hadoop rpc server!
			RpcServerThread rpcServerThread = new RpcServerThread( Integer.parseInt(temp[2]));
			Thread rS = new Thread(rpcServerThread);
			LOG.info("in remoteAPPMaster main start rpc server thread!");
			rS.start();
			LOG.info("in remoteAPPMaster main start rpc server thread success!");

			final String command = "hello";
			// Initialize clients to ResourceManager and NodeManagers
			LOG.info("go to start the amrmclient in main to allocate resource");
			//conf.set("yarn.resourcemanager.hostname","114.212.87.91");
			AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();

			String remoteYarnIp = temp[1];
			LOG.info("in remoteAPPMaster main,remoteYarnIp:"+remoteYarnIp);
			Configuration myConfiguration = new YarnConfiguration();
			myConfiguration.set(yraLabel,remoteYarnIp+":8032");
			myConfiguration.set(yrsaLabel,remoteYarnIp+":8030");
			rmClient.init(myConfiguration);
			rmClient.start();
			// Register with ResourceManager
			LOG.info("registerApplicationMaster ...");
			rmClient.registerApplicationMaster("", 0, "");
			LOG.info("registerApplicationMaster success!");


			while (!IsUnRegister) {
				//System.out.println("begin a container work !");
				LOG.info("in remoteAPPMaster am main while1 !");
				LOG.info("isNewRequest: "+isNewRequest);
				if(isNewRequest){
					LOG.info("in remoteAPPMaster am main while2 !");
					int i = 0;
					List<AllocateResponse> listRsp = new ArrayList<AllocateResponse>();
					int numOfAllRequest = 0;
					List<ResourceRequest> resourceRequestList = requestTmp.getAskList();
					for(Iterator it2 = resourceRequestList.iterator(); it2.hasNext();) {

						ResourceRequest temp1 = (ResourceRequest)it2.next();
						numOfAllRequest+=temp1.getNumContainers();

					}
					rmClient.addAllocateRequest(requestTmp);
					rmClient.addAllocateRequest(requestTmp);
					int rep = 0;
					while(i<numOfAllRequest*2) {
						AllocateResponse  allocateResponse = rmClient.allocate(++rep);
						listRsp.add(allocateResponse);
						i += allocateResponse.getAllocatedContainers().size();
						LOG.info("in remoteAPPMaster am main while3, response container: "+i
								+" request: "+numOfAllRequest*2);
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

		private int amRPCServerHost = 9999;
		private static final Log LOG = LogFactory
				.getLog(clusterMake.class);

		protected static Configuration myConf2 = new YarnConfiguration();
		public clusterMake(){
		}
		public clusterMake(Configuration conf, int amRPCServerHost){
			this.amRPCServerHost = amRPCServerHost;
			this.myConf2 = new YarnConfiguration(conf);
		}

		public void run() {

			try {
				this.useUMALauncher();
			} catch (Exception e) {
				e.printStackTrace();
			}
			LOG.info("finish thread in mySimpleAM");
		}


		private final String  hereYarnAddressLabel = "yarn.resourcemanager.hostname";

		public void useUMALauncher() throws Exception {
			LOG.info("!!!!!!!!!!!!!!!!!!!!!stop a while in useUMALauncher!!!!!!!!!!!!!!!!!!!!!!!!!!");

			String classpath = getTestRuntimeClasspath();
			//LOG.info("classpath:" +classpath);

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
							+ " success"+":"+myConf2.get(hereYarnAddressLabel)+":"+amRPCServerHost };
			LOG.info("remote yarn ip in the main args:"+
					" success"+":"+myConf2.get(hereYarnAddressLabel)+":"+amRPCServerHost);
			LOG.info("Initializing Launcher");
			UnmanagedAMLauncher launcher =
					new UnmanagedAMLauncher(myConf2);
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
		LOG.info("in ams allocate mergeResponseList ");

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

		int reponseId = 0;
		for(Iterator it2 = allReList.iterator(); it2.hasNext();){
			AllocateResponse allocateResponse = (AllocateResponse)it2.next();
			reponseId = allocateResponse.getResponseId();
			if(!allocateResponse.getCompletedContainersStatuses().isEmpty()) {
				completedContainers.addAll(allocateResponse.getCompletedContainersStatuses());
			}
			if(!allocateResponse.getUpdatedNodes().isEmpty()) {
				allUpdateNode.addAll(allocateResponse.getUpdatedNodes());
			}
			if(!allocateResponse.getAllocatedContainers().isEmpty()) {
				allAllocatedContainer.addAll(allocateResponse.getAllocatedContainers());
			}
			if(allocateResponse.getAvailableResources() != null) {
				allMen += allocateResponse.getAvailableResources().getMemory();
				allVCore += allocateResponse.getAvailableResources().getVirtualCores();
			}

			allCN += allocateResponse.getNumClusterNodes();

			if(!allocateResponse.getNMTokens().isEmpty()) {
				allNMTokens.addAll(allocateResponse.getNMTokens());
			}
			if(!allocateResponse.getIncreasedContainers().isEmpty()) {
				allCRI.addAll(allocateResponse.getIncreasedContainers());
			}
			if(!allocateResponse.getDecreasedContainers().isEmpty()) {
				allCRD.addAll(allocateResponse.getDecreasedContainers());
			}

		}


		Resource allAvailResource = Resource.newInstance(allMen,allVCore);


		AllocateResponse allResponse = AllocateResponse.newInstance(
				reponseId,
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
