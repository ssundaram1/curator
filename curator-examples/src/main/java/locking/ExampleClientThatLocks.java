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
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.curator.test.KillSession;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ExampleClientThatLocks
{
    private final InterProcessMutex lock;
    private final FakeLimitedResource resource;
    private final String clientName;

    public ExampleClientThatLocks(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName)
    {
        this.resource = resource;
        this.clientName = clientName;
        //lock = new InterProcessMutex(client, lockPath);
        //we wanna create persistent locks
         lock = new InterProcessMutex(client, lockPath, new StandardLockInternalsDriver()
        {
            @Override
            public String createsTheLock(CuratorFramework client, String path, byte[] lockNodeBytes) throws Exception
            {
                String ourPath;
                if ( lockNodeBytes != null )
                {
                    ourPath = client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.PERSISTENT).forPath(path, lockNodeBytes);
                }
                else
                {
                    ourPath = client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.PERSISTENT).forPath(path);
                }
                return ourPath;
            }
        });
    }

    public void doWork(long time, TimeUnit unit,String lockPath,CuratorFramework client) throws Exception
    {
        if ( !lock.acquire(time, unit) )
        {
            //throw new IllegalStateException
            System.out.println(clientName + " could not acquire the lock"+ lockPath);
            return;
        }
        try
        {
            System.out.println(clientName + " has the lock for path: "+lockPath);

            //delete all locks except ours
            List<String> participantNodes = new ArrayList<String>((List<String>)lock.getParticipantNodes());
            String actualLockPath = lock.getLockPath();
            System.out.println(clientName+" ACtual lock path"+actualLockPath);
//            if(participantNodes.size() > 1 ){
//                System.out.println(clientName+" multiple locks found  on lock path, locks: " +lock.getParticipantNodes()+" path:"+lockPath);
//                participantNodes.remove(actualLockPath);
//                Collections.sort(participantNodes);
//                Collections.reverse(participantNodes);
//                //sort in descending order
//                for(String participant :participantNodes){
//                        client.delete().guaranteed().inBackground().forPath(participant);
//
//
//                }
//                System.out.println(clientName+" reduced children on path, locks: " +lock.getParticipantNodes()+" path:"+lockPath);
//                //System.out.println(clientName+" reduced children on lock path @ ZK: locks:"+ client.getChildren().forPath(ZKPaths.makePath(lockPath,"lock") +" paths: "+ lockPath));
//
//            }

            String streamPath = lockPath.replace("extract","stream");


//            if(client.checkExists().forPath(streamPath) != null){
//                System.out.println(clientName +" stream node exists:"+streamPath);
//                return;
//
//            }

            System.out.println(clientName+" deleting own lock"+ lockPath);
            //client.delete().guaranteed().inBackground().forPath(actualLockPath);
            //lock.release();
                          System.out.println(clientName+" multiple locks found  on lock path, locks: " +lock.getParticipantNodes()+" path:"+lockPath);

            System.out.println(clientName+" Killing session!!" + lockPath);
            KillSession.kill(client.getZookeeperClient().getZooKeeper(), "localhost");


            System.out.println(clientName+" We won lets do this!!" + lockPath);



//            if(System.currentTimeMillis() % 2 == 0){
//                System.out.println(" Uh oh Client about to DIE");
//                client.close();
//            }
            //ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(),lockPath,false);
            //Thread.sleep(2000);

            //Thread.sleep(3000);

//            if(lock.getParticipantNodes().size() > 1 ){
//                System.out.println(clientName+" final children on path, locks: " +lock.getParticipantNodes()+" path:"+lockPath);
//                System.out.println(clientName+" final children on lock path @ ZK: locks:"+ client.getChildren().forPath(ZKPaths.makePath(lockPath,"lock") +" paths: "+ lockPath));
//
//            }


            //finally delete o1 for extract and create stream
//            client.create().creatingParentsIfNeeded().forPath(streamPath);
//            client.delete().deletingChildrenIfNeeded().inBackground().forPath(lockPath);
            System.out.println(clientName+ " AFTER DELETE Children for path and client: "+lockPath);



            //client.create().forPath(ZKPaths.makePath(lockPath,"Success"));
            //resource.use();
        }
        finally
        {

            //ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(),lockPath,false);

            lock.release(); // always release the lock in a finally block


            System.out.println(clientName + " releasing the lock");
        }
    }
}
