/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package locking;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.KillSession;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.*;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.KeeperException.CodeDeprecated.NodeExists;

public class LockingExample {
    private static final int QTY = 50;
    private static final int REPETITIONS = QTY * 10;

    private static final String PATH = "/extract";
    private static final String PATH2 = "/stream";
    BufferedWriter bw = null;
    FileWriter fw = null;

    public static void main(String[] args) throws Exception {
        // all of the useful sample code is in ExampleClientThatLocks.java

        final LockingExample test = new LockingExample();


        Timer timer = new Timer();

        timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {

                test.execute();

            }
        }, 0, 5000);

    }

    public void execute() {
        {
            // all of the useful sample code is in ExampleClientThatLocks.java

            // FakeLimitedResource simulates some external resource that can only be access by one process at a time
            final FakeLimitedResource resource = new FakeLimitedResource();
            ExampleClientThatLocks.orgProvCnt.clear();



            try {



                    File file = new File("testing1.txt");

                    // if file doesnt exists, then create it
                    if (!file.exists()) {
                        file.createNewFile();
                    }

                    // true = append file

                   fw = new FileWriter(file.getAbsoluteFile(), true);
                   bw = new BufferedWriter(fw);


                    ExecutorService service = Executors.newFixedThreadPool(QTY);
                // final TestingServer         server = new TestingServer();
                CuratorFramework mainClient = CuratorFrameworkFactory.newClient("localhost", new ExponentialBackoffRetry(1000, 3));
                mainClient.start();

                    if (mainClient.checkExists().forPath(PATH) != null) {
                        //CrudExamples.delete(mainClient,PATH);
                        ZKPaths.deleteChildren(mainClient.getZookeeperClient().getZooKeeper(), PATH, true);
                    }

                if (mainClient.checkExists().forPath(PATH2) != null) {
                    //CrudExamples.delete(mainClient,PATH);
                    ZKPaths.deleteChildren(mainClient.getZookeeperClient().getZooKeeper(), PATH2, true);
                }


                mainClient.create().withMode(CreateMode.PERSISTENT).forPath(PATH, null);
                for (int i = 0; i < 20; i++) {
                    mainClient.create().withMode(CreateMode.PERSISTENT).forPath(PATH + "/org" + i, null);
                }

                for (int i = 0; i < QTY; ++i) {
                    final int index = i;
                    Callable<Void> task = new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            CuratorFramework client = CuratorFrameworkFactory.newClient("localhost", new ExponentialBackoffRetry(1000, 3));

                            try {
                                client.start();
                                client.blockUntilConnected();
                                List<String> children = client.getChildren().forPath(PATH);
                                String clientName = "Client " + index;

                                // Thread.sleep(2000);
                                for (String path : children) {
                                    String childPath = ZKPaths.makePath(PATH, path);


                                    ExampleClientThatLocks example = new ExampleClientThatLocks(client, childPath, resource, clientName);
                                    example.doWork(0, TimeUnit.SECONDS, childPath, client);


                                }

                                //}
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } catch (Exception e) {
                                e.printStackTrace();
                                // log or do something
                            } finally {
                                CloseableUtils.closeQuietly(client);
                            }
                            return null;
                        }
                    };
                    service.submit(task);
                }

                service.shutdown();
                service.awaitTermination(10, TimeUnit.MINUTES);

                System.out.println(" org counts are : "+ExampleClientThatLocks.getProvCnt());
                System.out.println(" total org count : "+ExampleClientThatLocks.getProvCnt().keySet().size()+" "+System.currentTimeMillis());
                bw.write(" org counts are : "+ExampleClientThatLocks.getProvCnt()+"\n");
                bw.write(" total org count : "+ExampleClientThatLocks.getProvCnt().keySet().size()+" "+System.currentTimeMillis()+"\n");


                System.out.println("Main client querying: " + mainClient.getChildren().forPath("/"));


                    //more code

            } catch (InterruptedException e) {
                e.printStackTrace();
            }  catch (IOException e) {

                e.printStackTrace();

            }
            catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {

                    if (bw != null)
                        bw.close();

                    if (fw != null)
                        fw.close();

                } catch (IOException ex) {

                    ex.printStackTrace();

                }
                //CloseableUtils.closeQuietly("loc");

            }
        }

    }
}

