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

import framework.CrudExamples;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LockingExample
{
    private static final int        QTY = 10;
    private static final int        REPETITIONS = QTY * 10;

    private static final String     PATH = "/extract";

    public static void main(String[] args) throws Exception
    {
        // all of the useful sample code is in ExampleClientThatLocks.java

        // FakeLimitedResource simulates some external resource that can only be access by one process at a time
        final FakeLimitedResource   resource = new FakeLimitedResource();



        ExecutorService             service = Executors.newFixedThreadPool(QTY);
       // final TestingServer         server = new TestingServer();
        CuratorFramework        mainClient = CuratorFrameworkFactory.newClient("localhost", new ExponentialBackoffRetry(1000, 3));
        mainClient.start();
        if(mainClient.checkExists().forPath(PATH) != null){
            //CrudExamples.delete(mainClient,PATH);
            ZKPaths.deleteChildren(mainClient.getZookeeperClient().getZooKeeper(),PATH,true);
        }

        mainClient.create().withMode(CreateMode.PERSISTENT).forPath(PATH,null);
        for(int i =0; i<10;i++){
            mainClient.create().withMode(CreateMode.PERSISTENT).forPath(PATH+"/org"+i,null);
        }



        try
        {
            for ( int i = 0; i < QTY; ++i )
            {
                final int       index = i;
                Callable<Void>  task = new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        CuratorFramework   client = CuratorFrameworkFactory.newClient("localhost", new ExponentialBackoffRetry(1000, 3));

                        try
                        {
                            client.start();
                            client.blockUntilConnected();
                            List<String> children = client.getChildren().forPath(PATH);
                            String clientName ="Client " + index;

                            // Thread.sleep(2000);
                            for(String path: children) {
                                    String childPath = ZKPaths.makePath(PATH,path);
                                    String lockNodePath = ZKPaths.makePath(childPath,"lock");
                                    String streamPath = childPath.replace("extract","stream");

//                                    if(client.checkExists().forPath(streamPath) != null){
//                                        System.out.println(client +" stream node exists:"+streamPath);
//                                        continue;
//
//                                    }
                                //Leave this alone
                                if(client.checkExists().forPath(lockNodePath) != null &&  !client.getChildren().forPath(lockNodePath).isEmpty()){
                                    System.out.println(clientName +" lockNodePath node exists and has a lock node taken:"+lockNodePath);
                                    continue;
                                }
                                if(client.checkExists().forPath(streamPath) != null){
                                    System.out.println(clientName +" stream node exists:"+streamPath);
                                    continue;

                                }



                                        ExampleClientThatLocks example = new ExampleClientThatLocks(client,childPath, resource, clientName);
                                        example.doWork(0, TimeUnit.SECONDS, childPath, client);



                            }

                            //}
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                        }
                        catch ( Exception e )
                        {
                            e.printStackTrace();
                            // log or do something
                        }
                        finally
                        {
                            CloseableUtils.closeQuietly(client);
                        }
                        return null;
                    }
                };
                service.submit(task);
            }

            service.shutdown();
            service.awaitTermination(10, TimeUnit.MINUTES);

                       System.out.println("Main client querying: "+ mainClient.getChildren().forPath("/"));

        }
        finally
        {
            //CloseableUtils.closeQuietly(server);
        }
    }
}
