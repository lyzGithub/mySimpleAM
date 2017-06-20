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

package com.application.myApp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import com.application.api.AMRMClient;
import com.application.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.net.URL;
import java.util.Collections;


public class mySimpleAM implements Runnable{

    private static final Log LOG = LogFactory
            .getLog(mySimpleAM.class);

    protected static Configuration conf = new YarnConfiguration();
    public mySimpleAM(){

    }
    public mySimpleAM(Configuration conf){
        this.conf = conf;
    }
    public void run() {

        try {
            this.testUMALauncher();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("finish thread in mySimpleAM");
    }



    public void testUMALauncher() throws Exception {
        LOG.info("!!!!!!!!!!!!!!!!!!!!!stop a while in testUMALauncher!!!!!!!!!!!!!!!!!!!!!!!!!!");

        String classpath = getTestRuntimeClasspath();
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
                        + mySimpleAM.class.getCanonicalName()
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
    // provide main method so this class can act as AM
    public static void main(String[] args) throws Exception {

        LOG.info("!!!!!!!!!!!!!!!!!!!!!stop a while in main!!!!!!!!!!!!!!!!!!!!!!!!!!");
       // Thread.sleep(200);
        if (args[0].equals("success")) {

            final String command = "hello";
            final int n = 4;
            //final String command = args[0];
            // final int n = Integer.valueOf(args[1]);
            // Initialize clients to ResourceManager and NodeManagers
            Configuration myConf = new YarnConfiguration();
            LOG.info("set the yarn address !!! ");
            myConf.set("yarn.resourcemanager.address","0.0.0.0:8032");
            myConf.set("yarn.resourcemanager.scheduler.address","0.0.0.0:8030");
            //conf.set("yarn.resourcemanager.hostname","114.212.87.91");
            AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
            rmClient.init(myConf);
            rmClient.start();
            // Register with ResourceManager
            LOG.info("registerApplicationMaster ...");
            rmClient.registerApplicationMaster("", 0, "");
            LOG.info("registerApplicationMaster success!");
            Thread.sleep(2000);

            NMClient nmClient = NMClient.createNMClient();
            nmClient.init(myConf);
            nmClient.start();


            // Priority for worker containers - priorities are intra-application
            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(0);

            // Resource requirements for worker containers
            Resource capability = Records.newRecord(Resource.class);
            capability.setMemory(128);
            capability.setVirtualCores(1);

            // Make container requests to ResourceManager

            for(int i = 0; i<n; i++) {
                AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
                LOG.info("Making res-req " + (i));

                if((i%2) == 0) {
                    LOG.info("in simple am add hdfs1");
                    rmClient.addContainerRequest(containerAsk);//,"hdfs1");
                }
                else {
                    LOG.info("in simple am add local");
                    rmClient.addContainerRequest(containerAsk);
                }
            }

            AllocateRequest allocateRequest = null;
            //allocateRequest = AllocateRequest.newInstance();
            // Obtain allocated containers, launch and check for responses
            int responseId = 0;
            int completedContainers = 0;
            while (completedContainers<n) {
                LOG.info("begin a container work !");
                AllocateResponse response = rmClient.allocate(responseId++);
                for (Container container : response.getAllocatedContainers()) {
                    // Launch container by create ContainerLaunchContext
                    ContainerLaunchContext ctx =
                            Records.newRecord(ContainerLaunchContext.class);
                    ctx.setCommands(
                            Collections.singletonList(
                                    command +
                                            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                            ));
                    LOG.info("Launching container " + container.getId());
                    nmClient.startContainer(container, ctx);
                }
                for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                    LOG.info("Completed container " + status.getContainerId());
                    completedContainers++;
                    vetex = completedContainers;
                }
                Thread.sleep(1000);
                if(responseId>60){
                    LOG.error("response container is error  !");
                    break;
                }
            }
            LOG.info("Go to Finish am !");
            System.out.println("Go to Finish am !" );
            // Un-register with ResourceManager
            rmClient.unregisterApplicationMaster(
                    FinalApplicationStatus.SUCCEEDED, "", "");
            LOG.info("Finish running am !");
            System.out.println("Finish running am !" );
            System.exit(0);
        } else {
            System.exit(1);
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
        URL url = thisClassLoader.getResource("yarn-site.xml");
        envClassPath += new File(url.getFile()).getParent();
        return envClassPath;
    }
}
