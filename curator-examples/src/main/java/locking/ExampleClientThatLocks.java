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
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.concurrent.TimeUnit;

public class ExampleClientThatLocks
{
    private final FakeLimitedResource resource;
    private final String clientName;

    public ExampleClientThatLocks(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName)
    {
        this.resource = resource;
        this.clientName = clientName;
        //lock = new InterProcessMutex(client, lockPath);
        //we wanna create persistent locks
//         lock = new InterProcessMutex(client, lockPath, new StandardLockInternalsDriver()
//        {
//            @Override
//            public String createsTheLock(CuratorFramework client, String path, byte[] lockNodeBytes) throws Exception
//            {
//                String ourPath;
//                if ( lockNodeBytes != null )
//                {
//                    ourPath = client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.PERSISTENT).forPath(path, lockNodeBytes);
//                }
//                else
//                {
//                    ourPath = client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.PERSISTENT).forPath(path);
//                }
//                return ourPath;
//            }
//        });
    }

    public void doWork(long time, TimeUnit unit,String lockPath,CuratorFramework client) throws Exception
    {

            String lockedPath = ZKPaths.makePath(lockPath,"locked");
            String streamPath = lockPath.replace("extract", "stream");


            try {


                if(client.checkExists().forPath(lockedPath) == null){
                    System.out.println(clientName  +" won the lock "+lockPath);

                    if(client.checkExists().forPath(streamPath) != null){
                        System.out.println(clientName  +" STREAM node exists "+lockPath);
                        return;

                    }
                    client.create().withMode(CreateMode.EPHEMERAL).forPath(ZKPaths.makePath(lockPath,"locked"));
                    System.out.println(clientName + " LOCKED IT :"+lockedPath);
                    Thread.sleep(500);
                    //System.out.println(clientName+" Killing session!!" + lockedPath);
                    //KillSession.kill(client.getZookeeperClient().getZooKeeper(), "localhost");

                }else
                {
                    System.out.println(clientName+" Node already exists "+lockedPath);
                    return;
                }

            }catch(KeeperException.NodeExistsException e){

                System.out.println(clientName +" : Caught exception "+e.getMessage());
                return;
            }




            //finally delete o1 for extract and create stream
            client.create().creatingParentsIfNeeded().forPath(streamPath);
            client.delete().deletingChildrenIfNeeded().inBackground().forPath(lockPath);



            //client.create().forPath(ZKPaths.makePath(lockPath,"Success"));
            //resource.use();
        }
    }
