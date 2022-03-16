package heartbeat;

import services.CoordinationServices;
import messaging.ServerMessage;
import models.Server;
import org.json.simple.JSONObject;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import models.CurrentServer;
import services.LeaderServices;

import java.util.ArrayList;

import static services.CoordinationServices.sendServer;

public class ConsensusJob implements Job {
    private CurrentServer currentServer = CurrentServer.getInstance();
    private LeaderServices leaderServices = LeaderServices.getInstance();
    private ServerMessage serverMessage = ServerMessage.getInstance();

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        if (!currentServer.onGoingConsensus().get()) {
            // This is a leader based Consensus.
            // If no leader elected at the moment then no consensus task to perform.
            if (leaderServices.isLeaderElected()) {
                currentServer.onGoingConsensus().set(true);
                performConsensus(context); // critical region
                currentServer.onGoingConsensus().set(false);
            }
        } else {
            System.out.println("[SKIP] There seems to be on going consensus at the moment, skip.");
        }
    }

    private void performConsensus(JobExecutionContext context) {
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        String consensusVoteDuration = dataMap.get("consensusVoteDuration").toString();

        Integer suspectServerId = null;

        // initialise vote set
        currentServer.getVoteSet().put("YES", 0);
        currentServer.getVoteSet().put("NO", 0);

        Integer leaderServerId = leaderServices.getLeaderID();
        Integer myServerId = currentServer.getSelfID();

        // if I am leader, and suspect someone, I want to start voting to KICK him!
        if (myServerId.equals(leaderServerId)) {

            // find the next suspect to vote and break the loop
            for (Integer serverId : currentServer.getSuspectList().keySet()) {
                if (currentServer.getSuspectList().get(serverId).equals("SUSPECTED")) {
                    suspectServerId = serverId;
                    break;
                }
            }

            ArrayList<Server> serverList = new ArrayList<>();
            for (Integer serverid : currentServer.getServers().keySet()) {
                if (!serverid.equals(currentServer.getSelfID()) && currentServer.getSuspectList().get(serverid).equals("NOT_SUSPECTED")) {
                    serverList.add(currentServer.getServers().get(serverid));
                }
            }

            //got a suspect
            if (suspectServerId != null) {

                currentServer.getVoteSet().put("YES", 1); // I suspect it already, so I vote yes.
                JSONObject startVoteMessage = new JSONObject();
                startVoteMessage = serverMessage.startVoteMessage(currentServer.getSelfID(), suspectServerId);
                try {
                    CoordinationServices.sendServerBroadcast(startVoteMessage, serverList);
                    System.out.println("INFO : Leader calling for vote to kick suspect-server: " + startVoteMessage);
                } catch (Exception e) {
                    System.out.println("WARN : Leader calling for vote to kick suspect-server is failed");
                }

                //wait for consensus vote duration period
                try {
                    Thread.sleep(Integer.parseInt(consensusVoteDuration) * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                System.out.println((String.format("INFO : Consensus votes to kick server [%s]: %s", suspectServerId, currentServer.getVoteSet())));

                if (currentServer.getVoteSet().get("YES") > currentServer.getVoteSet().get("NO")) {

                    JSONObject notifyServerDownMessage = new JSONObject();
                    notifyServerDownMessage = serverMessage.notifyServerDownMessage(suspectServerId);
                    try {

                        CoordinationServices.sendServerBroadcast(notifyServerDownMessage, serverList);
                        System.out.println("INFO : Notify server " + suspectServerId + " down. Removing...");
//                        serverState.removeServer(suspectServerId);
                        leaderServices.removeRemoteChatRoomsClientsByServerId(suspectServerId);
                        currentServer.removeServerInCountList(suspectServerId);
                        currentServer.removeServerInSuspectList(suspectServerId);

                    } catch (Exception e) {
                        System.out.println("ERROR : " + suspectServerId + "Removing is failed");
                    }

                    System.out.println("INFO : Number of servers in group: " + currentServer.getServers().size());
                }
            }
        }
    }

    public static void startVoteMessageHandler(JSONObject j_object){

        CurrentServer currentServer = CurrentServer.getInstance();
        ServerMessage serverMessage = ServerMessage.getInstance();

        Integer suspectServerId = (int) (long)j_object.get("suspectServerId");
        Integer serverId = (int) (long)j_object.get("serverId");
        Integer mySeverId = currentServer.getSelfID();

        if (currentServer.getSuspectList().containsKey(suspectServerId)) {
            if (currentServer.getSuspectList().get(suspectServerId).equals("SUSPECTED")) {

                JSONObject answerVoteMessage = new JSONObject();
                answerVoteMessage = serverMessage.answerVoteMessage(suspectServerId, "YES", mySeverId);
                try {
                    sendServer(answerVoteMessage, currentServer.getServers().get(LeaderServices.getInstance().getLeaderID()));
                    System.out.println(String.format("INFO : Voting on suspected server: [%s] vote: YES", suspectServerId));
                } catch (Exception e) {
                    System.out.println("ERROR : Voting on suspected server is failed");
                }

            } else {

                JSONObject answerVoteMessage = new JSONObject();
                answerVoteMessage = serverMessage.answerVoteMessage(suspectServerId, "NO", mySeverId);
                try {
                    sendServer(answerVoteMessage, currentServer.getServers().get(LeaderServices.getInstance().getLeaderID()));
                    System.out.println(String.format("INFO : Voting on suspected server: [%s] vote: NO", suspectServerId));
                } catch (Exception e) {
                    System.out.println("ERROR : Voting on suspected server is failed");
                }
            }
        }

    }

    public static void answerVoteHandler(JSONObject j_object){

        CurrentServer currentServer = CurrentServer.getInstance();

        Integer suspectServerId = (int) (long)j_object.get("suspectServerId");
        String vote = (String) j_object.get("vote");
        Integer votedBy = (int) (long)j_object.get("votedBy");

        Integer voteCount = currentServer.getVoteSet().get(vote);

        System.out.println(String.format("Receiving voting to kick [%s]: [%s] voted by server: [%s]", suspectServerId, vote, votedBy));

        if (voteCount == null) {
            currentServer.getVoteSet().put(vote, 1);
        } else {
            currentServer.getVoteSet().put(vote, voteCount + 1);
        }

    }

    public static void notifyServerDownMessageHandler(JSONObject j_object){

        CurrentServer currentServer = CurrentServer.getInstance();
        LeaderServices leaderServices = LeaderServices.getInstance();

        Integer serverId = (int) (long)j_object.get("serverId");

        System.out.println("Server down notification received. Removing server: " + serverId);

//        serverState.removeServer(serverId);
        leaderServices.removeRemoteChatRoomsClientsByServerId(serverId);
        currentServer.removeServerInCountList(serverId);
        currentServer.removeServerInSuspectList(serverId);
    }

}
