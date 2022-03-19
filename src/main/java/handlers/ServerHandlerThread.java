package handlers; //ServerHandlerThread

import consensus.FastBully;
import consensus.LeaderStateUpdate;
import heartbeat.ConsensusJob;
import heartbeat.GossipJob;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import services.CoordinationServices;
import messaging.ServerMessage;
import models.Client;
import models.Server;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import models.CurrentServer;
import services.LeaderServices;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static util.Utils.send;

public class ServerHandlerThread extends Thread {
    private final ServerSocket CoordinationSocket;
    private LeaderStateUpdate leaderStateUpdate = new LeaderStateUpdate();

    public ServerHandlerThread(ServerSocket CoordinationSocket) {
        this.CoordinationSocket = CoordinationSocket;
    }

    public void clientIDApprovalRequest(String clientID, int sender, String threadID){

        try {
            JSONObject reply = new JSONObject();
            reply.put("type", "clientidapprovalreply");
            reply.put("threadid", threadID);

            if (LeaderServices.getInstance().isClientRegistered(clientID)) {
                reply.put("approved", "false");

                System.out.println(clientID + " from server s" + sender + " is not " + "approved");
            }
            else {
                Client client = new Client(clientID, CurrentServer.getMainHallIDbyServerInt(sender), null);
                LeaderServices.getInstance().addClient(client);
                reply.put("approved", "true");

                System.out.println(clientID + " from server s" + sender + " is " + "approved");
            }

            Server destinationServer = CurrentServer.getInstance().getServers().get(sender);

            // send approval reply to sender
            Socket destinationSocket = new Socket(destinationServer.getServerAddress(), destinationServer.getCoordinationPort());

            send(reply, destinationSocket);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //todo: check
    public void clientIDApprovalReply(Long threadID, boolean approved) {
        // non leader processes client ID approval request reply received
        ClientThreadHandler clientThreadHandler = CurrentServer.getInstance().getClientHandlerThread(threadID);

        if (approved) {
            clientThreadHandler.setApprovedClientID(1);
        }
        else {
            clientThreadHandler.setApprovedClientID(0);
        }

        Object lock = clientThreadHandler.getLock(); //todo: check why this lock is required
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    //todo: check
    public void roomCreateApprovalRequest(String clientID, String roomID, int sender,  String threadID) {
        // leader processes room create approval request received

        try {
            JSONObject reply = new JSONObject();
            reply.put("type", "roomcreateapprovalreply");
            reply.put("threadid", threadID);

            if (LeaderServices.getInstance().isRoomCreated(roomID)) {
                reply.put("approved", "false");
                System.out.println(roomID + " room creation request is not approved");
            } else {
                LeaderServices.getInstance().addApprovedRoom(clientID, roomID, sender);
                reply.put("approved", "true");
                System.out.println(roomID + " room creation request is approved");
            }

            Server destinationServer = CurrentServer.getInstance().getServers().get(sender);

            Socket destinationSocket = new Socket(destinationServer.getServerAddress(), destinationServer.getCoordinationPort());

            send(reply, destinationSocket);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //todo: check
    public void roomCreateApprovalReply(Long threadID, boolean approved) {
        // non leader processes room create approval request reply received
        ClientThreadHandler clientThreadHandler = CurrentServer.getInstance().getClientHandlerThread(threadID);

        if (approved) {
            clientThreadHandler.setApprovedRoomCreation(1);
        }
        else {
            clientThreadHandler.setApprovedRoomCreation(0);
        }

        Object lock = clientThreadHandler.getLock();
        synchronized (lock) { //todo: check this lock: what is it for?
            lock.notifyAll();
        }
    }

    //todo: check
    public void joinRoomApprovalRequest(
            String clientID,
            String roomID,
            String formerRoomID,
            int sender,
            String threadID,
            boolean isLocalRoomChange
    ) {
        // leader processes join room approval request received

        if (isLocalRoomChange) {
            //local change update leader
            Client client = new Client(clientID, roomID, null);
            LeaderServices.getInstance().localJoinRoomClient(client, formerRoomID);
        }
        else {
            try {
                int roomServerID = LeaderServices.getInstance().getServerIdIfRoomExist(roomID);

                Server destinationServer = CurrentServer.getInstance().getServers().get(sender);
                Server roomServer = CurrentServer.getInstance().getServers().get(roomServerID);

                JSONObject reply = new JSONObject();
                reply.put("type", "joinroomapprovalreply");
                reply.put("threadid", threadID);

                if (roomServerID != -1) {
                    LeaderServices.getInstance().removeClient(clientID, formerRoomID);//remove before route, later add on move join

                    reply.put("approved", "true");
                    reply.put("host", roomServer.getServerAddress());
                    reply.put("port", String.valueOf(roomServer.getClientsPort()));

                    System.out.println(roomID + " room join is approved");
                } else {
                    reply.put("approved", "false");
                    reply.put("host", "");
                    reply.put("port", "");

                    System.out.println(roomID + " room join is not approved");
                }

                Socket destinationSocket = new Socket(destinationServer.getServerAddress(), destinationServer.getCoordinationPort());
                send(reply, destinationSocket);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //todo: check
    public void joinRoomApprovalReply(boolean approved, Long threadID, String host, String port) {
        // non leader processes room create approval request reply received

        int intApproved;
        if (approved) {
            intApproved = 1;
        } else {
            intApproved = 0;
        }

        ClientThreadHandler clientThreadHandler = CurrentServer.getInstance().getClientHandlerThread(threadID);

        Object lock = clientThreadHandler.getLock();

        synchronized (lock) {
            clientThreadHandler.setApprovedJoinRoom(intApproved);
            clientThreadHandler.setApprovedJoinRoomServerHostAddress(host);
            clientThreadHandler.setApprovedJoinRoomServerPort(port);
            lock.notifyAll();
        }
    }

    //todo: check
    public void moveJoinAck(String clientID, String roomID, int sender) {
        //leader process move join acknowledgement from the target room server after change
        Client client = new Client(clientID, roomID, null);
        LeaderServices.getInstance().addClient(client);

        System.out.println(clientID + " client move to to " + roomID +  " room of server s" + sender);
    }

    //todo: check
    public void listRequest(String threadID, int sender) {
        try {
            //leader process list request
            Server destServer = CurrentServer.getInstance().getServers().get(sender);

            CoordinationServices.sendServer(
                    ServerMessage.getListResponse(LeaderServices.getInstance().getRoomIDList(), threadID),
                    destServer
            );
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    //todo: check
    public void listResponse(Long threadID, JSONArray roomsJSONArray) {
        ArrayList<String> roomIDs = new ArrayList();
        roomIDs.addAll(roomsJSONArray);

        ClientThreadHandler clientThreadHandler = CurrentServer.getInstance().getClientHandlerThread(threadID);

        Object lock = clientThreadHandler.getLock();

        synchronized (lock) {
            clientThreadHandler.setRoomsList(roomIDs);
            lock.notifyAll();
        }
    }

    //todo: check
    public void deleteRequest(String roomID, String mainHallID, String ownerID){
        LeaderServices.getInstance().removeRoom(roomID, mainHallID, ownerID);
    }

    //todo: check
    public void quit(String clientID, String former) {
        // leader removes client from global room list
        LeaderServices.getInstance().removeClient(clientID, former);
        System.out.println("leader delete " + clientID + " client ");
    }

    //todo: check
    public void leaderStateUpdate(JSONObject data) {
        if(LeaderServices.getInstance().isLeaderElectedAndIamLeader()) {
            if(!leaderStateUpdate.isAlive()) {
                leaderStateUpdate = new LeaderStateUpdate();
                leaderStateUpdate.start();
            }
            leaderStateUpdate.receiveUpdate(data);
        }
    }

    //todo: check
    public void leaderStateUpdateComplete(int serverID) {
        if(LeaderServices.getInstance().isLeaderElectedAndMessageFromLeader(serverID)) {
            System.out.println("Received leader update complete message from s"+serverID);
            FastBully.leaderUpdateComplete = true;
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                Socket serverSocket = CoordinationSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));
                String serverInputLine = in.readLine();

                JSONParser jsonParser = new JSONParser();
                JSONObject serverInputData = (JSONObject) jsonParser.parse(serverInputLine);

                if (serverInputData.containsKey("option")) {
                    // messages with 'option' tag will be handled inside Fast Bully Algorithm
                    FastBully.receiveMessages(serverInputData);
                }
                else if (serverInputData.containsKey("type")) {

                    switch (serverInputData.get("type").toString()) {
                        case "clientidapprovalrequest" -> clientIDApprovalRequest(
                                serverInputData.get("clientid").toString(),
                                Integer.parseInt(serverInputData.get("sender").toString()),
                                serverInputData.get("threadid").toString()
                        );

                        case "clientidapprovalreply" -> clientIDApprovalReply(
                                Long.parseLong(serverInputData.get("threadid").toString()),
                                Boolean.parseBoolean(serverInputData.get("approved").toString())
                        );

                        case "roomcreateapprovalrequest" -> roomCreateApprovalRequest(
                                serverInputData.get("clientid").toString(),
                                serverInputData.get("roomid").toString(),
                                Integer.parseInt(serverInputData.get("sender").toString()),
                                serverInputData.get("threadid").toString()
                        );

                        case "roomcreateapprovalreply" -> roomCreateApprovalReply(
                                Long.parseLong(serverInputData.get("threadid").toString()),
                                Boolean.parseBoolean(serverInputData.get("approved").toString())
                        );

                        case "joinroomapprovalrequest" -> joinRoomApprovalRequest(
                                serverInputData.get("clientid").toString(),
                                serverInputData.get("roomid").toString(),
                                serverInputData.get("former").toString(),
                                Integer.parseInt(serverInputData.get("sender").toString()),
                                serverInputData.get("threadid").toString(),
                                Boolean.parseBoolean(serverInputData.get("isLocalRoomChange").toString())
                        );


                        case "joinroomapprovalreply" -> joinRoomApprovalReply(
                                Boolean.parseBoolean(serverInputData.get("approved").toString()),
                                Long.parseLong(serverInputData.get("threadid").toString()),
                                serverInputData.get("host").toString(),
                                serverInputData.get("port").toString()
                        );

                        case "movejoinack" -> moveJoinAck(
                                serverInputData.get("clientid").toString(),
                                serverInputData.get("roomid").toString(),
                                Integer.parseInt(serverInputData.get("sender").toString())
                        );

                        case "listrequest" -> listRequest(
                                serverInputData.get("threadid").toString(),
                                Integer.parseInt(serverInputData.get("sender").toString())
                        );

                        case "listresponse" -> listResponse(
                                Long.parseLong(serverInputData.get("threadid").toString()),
                                (JSONArray) serverInputData.get("rooms")
                        );

                        case "deleterequest" -> deleteRequest(
                                serverInputData.get("roomid").toString(),
                                serverInputData.get("mainhall").toString(),
                                serverInputData.get("owner").toString()
                        );

                        case "quit" -> quit(
                                serverInputData.get("clientid").toString(),
                                serverInputData.get("former").toString()
                        );

                        case "leaderstateupdate" -> leaderStateUpdate(serverInputData);

                        case "leaderstateupdatecomplete" -> leaderStateUpdateComplete(
                                Integer.parseInt(serverInputData.get("serverid").toString())
                        );

                        case "gossip" -> GossipJob.receiveMessages(serverInputData);

                        case "startVote" -> ConsensusJob.startVoteMessageHandler(serverInputData);

                        case "answervote" -> ConsensusJob.answerVoteHandler(serverInputData);

                        case "notifyserverdown" -> ConsensusJob.notifyServerDownMessageHandler(serverInputData);
                    }
                }
                else {
                    System.out.println("Invalid JSON from Server");
                }
                serverSocket.close();
            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }
}
