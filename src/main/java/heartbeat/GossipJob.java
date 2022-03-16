package heartbeat;

import services.LeaderServices;
import messaging.ServerMessage;
import org.json.simple.JSONObject;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import models.Server;
import models.CurrentServer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

import static services.CoordinationServices.sendServer;

public class GossipJob implements Job{
    private CurrentServer currentServer = CurrentServer.getInstance();
    private LeaderServices leaderServices = LeaderServices.getInstance();
    private ServerMessage serverMessage = ServerMessage.getInstance();

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        String aliveErrorFactor = dataMap.get("aliveErrorFactor").toString();

        // first work on heart beat vector and suspect failure server list

        for (Server serverInfo : currentServer.getServers().values()){
            Integer serverId = serverInfo.getServerID();
            Integer myServerId = currentServer.getSelfID();

            // get current heart beat count of a server
            Integer count = currentServer.getHeartbeatCountList().get(serverId);

            // first update heart beat count vector
            if (serverId.equals(myServerId)) {
                currentServer.getHeartbeatCountList().put(serverId, 0); // reset my own vector always
            } else {
                // up count all others
                if (count == null) {
                    currentServer.getHeartbeatCountList().put(serverId, 1);
                } else {
                    currentServer.getHeartbeatCountList().put(serverId, count + 1);
                }
            }

            // FIX get the fresh updated current count again
            count = currentServer.getHeartbeatCountList().get(serverId);

            if (count != null) {
                // if heart beat count is more than error factor
                if (count > Integer.parseInt(aliveErrorFactor)) {
                    currentServer.getSuspectList().put(serverId, "SUSPECTED");
                } else {
                    currentServer.getSuspectList().put(serverId, "NOT_SUSPECTED");
                }
            }

        }

        // next challenge leader election if a coordinator is in suspect list

//        if (leaderState.isLeaderElected()){
//
//            Integer leaderServerId = leaderState.getLeaderID();
//            System.out.println("Current coordinator is : " + leaderState.getLeaderID().toString());
//
//            // if the leader/coordinator server is in suspect list, start the election process
//            if (serverState.getSuspectList().get(leaderServerId).equals("SUSPECTED")) {
//
//                //initiate an election
//                BullyAlgorithm.initialize();
//            }
//        }

        // finally gossip about heart beat vector to a next peer

        int numOfServers = currentServer.getServers().size();

        if (numOfServers > 1) { // Gossip required at least 2 servers to be up

            // after updating the heartbeatCountList, randomly select a server and send
            Integer serverIndex = ThreadLocalRandom.current().nextInt(numOfServers - 1);
            ArrayList<Server> remoteServer = new ArrayList<>();
            for (Server server : currentServer.getServers().values()) {
                Integer serverId = server.getServerID();
                Integer myServerId = currentServer.getSelfID();
                if (!serverId.equals(myServerId)) {
                    remoteServer.add(server);
                }
            }
//            Collections.shuffle(remoteServer, new Random(System.nanoTime())); // another way of randomize the list

            // change concurrent hashmap to hashmap before sending
            HashMap<Integer, Integer> heartbeatCountList = new HashMap<>(currentServer.getHeartbeatCountList());
            JSONObject gossipMessage = new JSONObject();
            gossipMessage = serverMessage.gossipMessage(currentServer.getSelfID(), heartbeatCountList);
            try {
                sendServer(gossipMessage,remoteServer.get(serverIndex));
                System.out.println("INFO : Gossip heartbeat info to next peer s"+remoteServer.get(serverIndex).getServerID());
            } catch (Exception e){
                System.out.println("WARN : Server s"+remoteServer.get(serverIndex).getServerID() +
                        " has failed");
            }

        }

    }

    public static void receiveMessages(JSONObject j_object) {

        CurrentServer currentServer = CurrentServer.getInstance();

        HashMap<String, Long> gossipFromOthers = (HashMap<String, Long>) j_object.get("heartbeatCountList");
        Integer fromServer = (int) (long)j_object.get("serverId");

        System.out.println(("Receiving gossip from server: [" + fromServer.toString() + "] gossipping: " + gossipFromOthers));

        //update the heartbeatcountlist by taking minimum
        for (String serverId : gossipFromOthers.keySet()) {
            Integer localHeartbeatCount = currentServer.getHeartbeatCountList().get(Integer.parseInt(serverId));
            Integer remoteHeartbeatCount = (int) (long)gossipFromOthers.get(serverId);
            if (localHeartbeatCount != null && remoteHeartbeatCount < localHeartbeatCount) {
                currentServer.getHeartbeatCountList().put(Integer.parseInt(serverId), remoteHeartbeatCount);
            }
        }

        System.out.println(("Current cluster heart beat state is: " + currentServer.getHeartbeatCountList()));

        if (LeaderServices.getInstance().isLeaderElected() && LeaderServices.getInstance().getLeaderID().equals(currentServer.getSelfID())) {
            if (currentServer.getHeartbeatCountList().size() < gossipFromOthers.size()) {
                for (String serverId : gossipFromOthers.keySet()) {
                    if (!currentServer.getHeartbeatCountList().containsKey(serverId)) {
                        currentServer.getSuspectList().put(Integer.parseInt(serverId), "SUSPECTED");
                    }
                }
            }
        }

    }
}
