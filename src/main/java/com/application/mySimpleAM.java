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

package com.application;

import com.application.UmAM.MiniYARNCluster;
import com.application.UmAM.UnmanagedAMLauncher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
//import com.application.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;

import java.io.*;
import java.net.URL;
import java.util.Collections;

import static org.junit.Assert.assertTrue;

//import org.apache.hadoop.yarn.server.MiniYARNCluster;

public class mySimpleAM implements Runnable{//
    private static final Log LOG = LogFactory
            .getLog(mySimpleAM.class);

    protected static MiniYARNCluster yarnCluster = null;
    protected static Configuration conf = new YarnConfiguration();
    public void run() {

        try {
            this.setup();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            this.testUMALauncher();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            this.tearDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("finish thread");
    }

    //this setup is set up the running env for application master
    //@BeforeClass
    public static void setup() throws InterruptedException, IOException {
        System.out.println("!!!!!!!!!!!!!!!!!!!!!stop a while in setup!!!!!!!!!!!!!!!!!!!!!!!!!!");
        //Thread.sleep(200);
        LOG.info("Starting up YARN cluster");
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
        if (yarnCluster == null) {
            //System.out.println(mySimpleAM.class.getSimpleName());//mySimpleAM

            yarnCluster = new MiniYARNCluster(
                    mySimpleAM.class.getSimpleName(), 1, 1, 1);
            yarnCluster.init(conf);
            yarnCluster.start();
            //get the address
            Configuration yarnClusterConfig = yarnCluster.getConfig();
            LOG.info("MiniYARN ResourceManager published address: " +
                    yarnClusterConfig.get(YarnConfiguration.RM_ADDRESS));
            LOG.info("MiniYARN ResourceManager published web address: " +
                    yarnClusterConfig.get(YarnConfiguration.RM_WEBAPP_ADDRESS));
            String webapp = yarnClusterConfig.get(YarnConfiguration.RM_WEBAPP_ADDRESS);
            assertTrue("Web app address still unbound to a host at " + webapp,
                    !webapp.startsWith("0.0.0.0"));
            LOG.info("Yarn webapp is at "+ webapp);
            URL url = Thread.currentThread().getContextClassLoader()
                    .getResource("yarn-site.xml");
            if (url == null) {
                throw new RuntimeException(
                        "Could not find 'yarn-site.xml' dummy file in classpath");
            }
            //write the document to a buffer (not directly to the file, as that
            //can cause the file being written to get read -which will then fail.
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            yarnClusterConfig.writeXml(bytesOut);
            bytesOut.close();
            //write the bytes to the file in the classpath
            OutputStream os = new FileOutputStream(new File(url.getPath()));
            os.write(bytesOut.toByteArray());
            os.close();
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
        }
    }

    //@AfterClass
    public static void tearDown() throws IOException, Exception {
        System.out.println("!!!!!!!!!!!!!!!!!!!!!stop a while in tearDown!!!!!!!!!!!!!!!!!!!!!!!!!!");
        //Thread.sleep(200);
        if (yarnCluster != null) {
            try {
                System.out.println("stopping a yarncluster!");
                yarnCluster.stop();
                System.out.println("stop a yarnclusterff!");
            } finally {
                yarnCluster = null;
            }
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

    //@Test(timeout=30000)
    public void testUMALauncher() throws Exception {
        System.out.println("!!!!!!!!!!!!!!!!!!!!!stop a while in testUMALauncher!!!!!!!!!!!!!!!!!!!!!!!!!!");
       // Thread.sleep(200);
        String classpath = getTestRuntimeClasspath();
        //System.out.println("classpath:"+classpath);
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
                new UnmanagedAMLauncher(new Configuration(yarnCluster.getConfig())) {
                    public void launchAM(ApplicationAttemptId attemptId)
                            throws IOException, YarnException {
                        YarnApplicationAttemptState attemptState =
                                rmClient.getApplicationAttemptReport(attemptId)
                                        .getYarnApplicationAttemptState();
                        Assert.assertTrue(attemptState
                                .equals(YarnApplicationAttemptState.LAUNCHED));
                        super.launchAM(attemptId);
                    }
                };
        boolean initSuccess = launcher.init(args);
        Assert.assertTrue(initSuccess);
        LOG.info("Running Launcher");
        boolean result = launcher.run();

        LOG.info("Launcher run completed. Result=" + result);
        Assert.assertTrue(result);

    }


    // provide main method so this class can act as AM
    public static void main(String[] args) throws Exception {

        System.out.println("!!!!!!!!!!!!!!!!!!!!!stop a while in main!!!!!!!!!!!!!!!!!!!!!!!!!!");
       // Thread.sleep(200);
        if (args[0].equals("success")) {

            final String command = "hello";
            final int n = 2;
            //final String command = args[0];
            // final int n = Integer.valueOf(args[1]);
            // Initialize clients to ResourceManager and NodeManagers
            Configuration myConf = new YarnConfiguration();

            //conf.set("yarn.resourcemanager.hostname","114.212.87.91");
            AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
            rmClient.init(myConf);
            rmClient.start();
            // Register with ResourceManager
            System.out.println("registerApplicationMaster ...");
            rmClient.registerApplicationMaster("", 0, "");
            System.out.println("registerApplicationMaster success!");


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
            for (int i = 0; i < n; ++i) {
                AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
                System.out.println("Making res-req " + i);
                rmClient.addContainerRequest(containerAsk);
            }

            // Obtain allocated containers, launch and check for responses
            int responseId = 0;
            int completedContainers = 0;
            while (completedContainers < n) {
                //System.out.println("begin a container work !");
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
                    System.out.println("Launching container " + container.getId());
                    nmClient.startContainer(container, ctx);
                }
                for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                    ++completedContainers;
                    System.out.println("Completed container " + status.getContainerId());
                }
                //System.out.println("Do another container work !");
                Thread.sleep(100);
            }
            System.out.println("Go to Finish am !");
            // Un-register with ResourceManager
            rmClient.unregisterApplicationMaster(
                    FinalApplicationStatus.SUCCEEDED, "", "");
            System.out.println("Finish running am !");

            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
