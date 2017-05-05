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
import org.apache.curator.utils.ZKPaths;

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
        lock = new InterProcessMutex(client, lockPath);
    }

    public void     doWork(long time, TimeUnit unit,String lockPath,CuratorFramework client) throws Exception
    {
        if ( !lock.acquire(time, unit) )
        {
            throw new IllegalStateException(clientName + " could not acquire the lock");
        }
        try
        {
            System.out.println(clientName + " has the lock"+" for path: "+lockPath);
//            if(client.checkExists().forPath(ZKPaths.makePath(lockPath, "Success")) != null) {
//                    List<String > children =  client.getChildren().forPath(lockPath);
//                    System.out.println("path has sucess node already, releasing lock: "+clientName+" for path: "+lockPath);
//                    //ZKPaths.deleteChildren()..deleteChildren(client.getZookeeperClient().getZooKeeper(),lockPath,true);
//                    //release not needed here
//                    lock.release();
//
//                return;
//            }

            System.out.println("We won lets do this!!");

//            if(System.currentTimeMillis() % 2 == 0){
//                System.out.println(" Uh oh Client about to DIE");
//                client.close();
//            }
            //ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(),lockPath,false);
            //Thread.sleep(2000);


            //client.create().forPath(ZKPaths.makePath(lockPath,"Success"));
            //resource.use();
        }
        finally
        {

            //ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(),lockPath,false);
            client.delete().guaranteed().deletingChildrenIfNeeded().forPath(lockPath);
            System.out.println("AFTER DELETE Children for path and client: "+lockPath+" "+clientName);
            lock.release(); // always release the lock in a finally block
            System.out.println(clientName + " releasing the lock");
        }
    }
}
