package handlers; //ServerHandlerThread

import consensus.FastBully;
import consensus.LeaderStateUpdate;
import heartbeat.ConsensusJob;
import heartbeat.GossipJob;
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

public class ServerHandlerThread extends Thread {
    private final ServerSocket serverCoordinationSocket;
    private LeaderStateUpdate leaderStateUpdate = new LeaderStateUpdate();

    public ServerHandlerThread(ServerSocket serverCoordinationSocket) {
        this.serverCoordinationSocket = serverCoordinationSocket;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Socket serverSocket = serverCoordinationSocket.accept();
                BufferedReader bufferedReader = new BufferedReader(
                        new InputStreamReader(serverSocket.getInputStream(), StandardCharsets.UTF_8)
                );
                String jsonStringFromServer = bufferedReader.readLine();

                // convert received message to json object
                JSONObject j_object = CoordinationServices.convertToJson(jsonStringFromServer);


                if (j_object.containsKey("option")) {
                    // messages with 'option' tag will be handled inside BullyAlgorithm
                    FastBully.receiveMessages(j_object);
                } else if (j_object.containsKey("type")) {

                    if (j_object.get("type").equals("clientidapprovalrequest")
                            && j_object.get("clientid") != null && j_object.get("sender") != null
                            && j_object.get("threadid") != null) {

                        // leader processes client ID approval request received
                        String clientID = j_object.get("clientid").toString();
                        int sender = Integer.parseInt(j_object.get("sender").toString());
                        String threadID = j_object.get("threadid").toString();

                        boolean approved = !LeaderServices.getInstance().isClientRegistered(clientID);
                        if (approved) {
                            Client client = new Client(clientID,
                                    CurrentServer.getMainHallIDbyServerInt(sender),
                                    null);
                            LeaderServices.getInstance().addClient(client);
                        }
                        Server destServer = CurrentServer.getInstance().getServers()
                                .get(sender);
                        try {
                            // send client id approval reply to sender
                            CoordinationServices.sendServer(
                                    ServerMessage.getClientIdApprovalReply(String.valueOf(approved), threadID),
                                    destServer
                            );
                            System.out.println("INFO : Client ID '" + clientID +
                                    "' from s" + sender + " is" + (approved ? " " : " not ") + "approved");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    } else if (j_object.get("type").equals("clientidapprovalreply")
                            && j_object.get("approved") != null && j_object.get("threadid") != null) {

                        // non leader processes client ID approval request reply received
                        int approved = Boolean.parseBoolean(j_object.get("approved").toString()) ? 1 : 0;
                        Long threadID = Long.parseLong(j_object.get("threadid").toString());

                        ClientThreadHandler clientThreadHandler = CurrentServer.getInstance()
                                .getClientHandlerThread(threadID);
                        clientThreadHandler.setApprovedClientID(approved);
                        Object lock = clientThreadHandler.getLock();
                        synchronized (lock) {
                            lock.notifyAll();
                        }

                    } else if (j_object.get("type").equals("roomcreateapprovalrequest")) {

                        // leader processes room create approval request received
                        String clientID = j_object.get("clientid").toString();
                        String roomID = j_object.get("roomid").toString();
                        int sender = Integer.parseInt(j_object.get("sender").toString());
                        String threadID = j_object.get("threadid").toString();

                        boolean approved = LeaderServices.getInstance().isRoomCreated(roomID);

                        if (approved) {
                            LeaderServices.getInstance().addApprovedRoom(clientID, roomID, sender);
                        }
                        Server destServer = CurrentServer.getInstance().getServers()
                                .get(sender);
                        try {
                            // send room create approval reply to sender
                            CoordinationServices.sendServer(
                                    ServerMessage.getRoomCreateApprovalReply(String.valueOf(approved), threadID),
                                    destServer
                            );
                            System.out.println("INFO : Room '" + roomID +
                                    "' creation request from client " + clientID +
                                    " is" + (approved ? " " : " not ") + "approved");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else if (j_object.get("type").equals("roomcreateapprovalreply")) {

                        // non leader processes room create approval request reply received
                        int approved = Boolean.parseBoolean(j_object.get("approved").toString()) ? 1 : 0;
                        Long threadID = Long.parseLong(j_object.get("threadid").toString());

                        ClientThreadHandler clientThreadHandler = CurrentServer.getInstance()
                                .getClientHandlerThread(threadID);
                        clientThreadHandler.setApprovedRoomCreation(approved);
                        Object lock = clientThreadHandler.getLock();
                        synchronized (lock) {
                            lock.notifyAll();
                        }

                    } else if (j_object.get("type").equals("joinroomapprovalrequest")) {

                        // leader processes join room approval request received

                        //get params
                        String clientID = j_object.get("clientid").toString();
                        String roomID = j_object.get("roomid").toString();
                        String formerRoomID = j_object.get("former").toString();
                        int sender = Integer.parseInt(j_object.get("sender").toString());
                        String threadID = j_object.get("threadid").toString();
                        boolean isLocalRoomChange = Boolean.parseBoolean(j_object.get("isLocalRoomChange").toString());

                        if (isLocalRoomChange) {
                            //local change update leader
                            Client client = new Client(clientID, roomID, null);
                            LeaderServices.getInstance().localJoinRoomClient(client, formerRoomID);
                        } else {
                            int serverIDofTargetRoom = LeaderServices.getInstance().getServerIdIfRoomExist(roomID);

                            Server destServer = CurrentServer.getInstance().getServers().get(sender);
                            try {

                                boolean approved = serverIDofTargetRoom != -1;
                                if (approved) {
                                    LeaderServices.getInstance().removeClient(clientID, formerRoomID);//remove before route, later add on move join
                                }
                                Server serverOfTargetRoom = CurrentServer.getInstance().getServers().get(serverIDofTargetRoom);

                                String host = (approved) ? serverOfTargetRoom.getServerAddress() : "";
                                String port = (approved) ? String.valueOf(serverOfTargetRoom.getClientsPort()) : "";

                                CoordinationServices.sendServer(
                                        ServerMessage.getJoinRoomApprovalReply(
                                                String.valueOf(approved),
                                                threadID, host, port),
                                        destServer
                                );
                                System.out.println("INFO : Join Room from [" + formerRoomID +
                                        "] to [" + roomID + "] for client " + clientID +
                                        " is" + (serverIDofTargetRoom != -1 ? " " : " not ") + "approved");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    } else if (j_object.get("type").equals("joinroomapprovalreply")) {

                        // non leader processes room create approval request reply received
                        int approved = Boolean.parseBoolean(j_object.get("approved").toString()) ? 1 : 0;
                        Long threadID = Long.parseLong(j_object.get("threadid").toString());
                        String host = j_object.get("host").toString();
                        String port = j_object.get("port").toString();

                        ClientThreadHandler clientThreadHandler = CurrentServer.getInstance()
                                .getClientHandlerThread(threadID);

                        Object lock = clientThreadHandler.getLock();

                        synchronized (lock) {
                            clientThreadHandler.setApprovedJoinRoom(approved);
                            clientThreadHandler.setApprovedJoinRoomServerHostAddress(host);
                            clientThreadHandler.setApprovedJoinRoomServerPort(port);
                            lock.notifyAll();
                        }

                    } else if (j_object.get("type").equals("movejoinack")) {
                        //leader process move join acknowledgement from the target room server after change

                        //parse params
                        String clientID = j_object.get("clientid").toString();
                        String roomID = j_object.get("roomid").toString();
                        String formerRoomID = j_object.get("former").toString();
                        int sender = Integer.parseInt(j_object.get("sender").toString());
                        String threadID = j_object.get("threadid").toString();

                        Client client = new Client(clientID, roomID, null);
                        LeaderServices.getInstance().addClient(client);

                        System.out.println("INFO : Moved Client [" + clientID + "] to server s" + sender
                                + " and room [" + roomID + "] is updated as current room");
                    } else if (j_object.get("type").equals("listrequest")) {
                        //leader process list request

                        //parse params
                        String clientID = j_object.get("clientid").toString();
                        String threadID = j_object.get("threadid").toString();
                        int sender = Integer.parseInt(j_object.get("sender").toString());

                        Server destServer = CurrentServer.getInstance().getServers().get(sender);

                        CoordinationServices.sendServer(
                                ServerMessage.getListResponse(LeaderServices.getInstance().getRoomIDList(), threadID),
                                destServer
                        );
                    } else if (j_object.get("type").equals("listresponse")) {

                        Long threadID = Long.parseLong(j_object.get("threadid").toString());
                        JSONArray roomsJSONArray = (JSONArray) j_object.get("rooms");
                        ArrayList<String> roomIDList = new ArrayList(roomsJSONArray);

                        ClientThreadHandler clientThreadHandler = CurrentServer.getInstance()
                                .getClientHandlerThread(threadID);

                        Object lock = clientThreadHandler.getLock();

                        synchronized (lock) {
                            clientThreadHandler.setRoomsListTemp(roomIDList);
                            lock.notifyAll();
                        }
                    } else if (j_object.get("type").equals("deleterequest")) {
                        String ownerID = j_object.get("owner").toString();
                        String roomID = j_object.get("roomid").toString();
                        String mainHallID = j_object.get("mainhall").toString();

                        LeaderServices.getInstance().removeRoom(roomID, mainHallID, ownerID);

                    } else if (j_object.get("type").equals("quit")) {
                        String clientID = j_object.get("clientid").toString();
                        String formerRoomID = j_object.get("former").toString();
                        // leader removes client from global room list
                        LeaderServices.getInstance().removeClient(clientID, formerRoomID);
                        System.out.println("INFO : Client '" + clientID + "' deleted by leader");

                    } else if (j_object.get("type").equals("leaderstateupdate")) {
                        if( LeaderServices.getInstance().isLeaderElectedAndIamLeader() )
                        {
                            if( !leaderStateUpdate.isAlive() )
                            {
                                leaderStateUpdate = new LeaderStateUpdate();
                                leaderStateUpdate.start();
                            }
                            leaderStateUpdate.receiveUpdate( j_object );
                        }

                    } else if (j_object.get("type").equals("leaderstateupdatecomplete")) {
                        int serverID = Integer.parseInt(j_object.get("serverid").toString());
                        if( LeaderServices.getInstance().isLeaderElectedAndMessageFromLeader( serverID ) )
                        {
                            System.out.println("INFO : Received leader update complete message from s"+serverID);
                            FastBully.leaderUpdateComplete = true;
                        }

                    } else if (j_object.get("type").equals("gossip")) {
                        GossipJob.receiveMessages(j_object);

                    } else if (j_object.get("type").equals("startVote")) {
                        ConsensusJob.startVoteMessageHandler(j_object);

                    } else if (j_object.get("type").equals("answervote")) {
                        ConsensusJob.answerVoteHandler(j_object);

                    } else if (j_object.get("type").equals("notifyserverdown")) {
                        ConsensusJob.notifyServerDownMessageHandler(j_object);

                    } else {
                        System.out.println("WARN : Command error, Corrupted JSON from Server");
                    }
                } else {
                    System.out.println("WARN : Command error, Corrupted JSON from Server");
                }
                serverSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
