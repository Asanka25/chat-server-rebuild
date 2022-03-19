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

public class ServerHandlerThread extends Thread {
    private final ServerSocket CoordinationSocket;
    private LeaderStateUpdate leaderStateUpdate = new LeaderStateUpdate();

    public ServerHandlerThread(ServerSocket CoordinationSocket) {
        this.CoordinationSocket = CoordinationSocket;
    }

    public void clientIDApprovalRequest(JSONObject data){
        // leader processes client ID approval request received
        String clientID = data.get("clientid").toString();
        int sender = Integer.parseInt(data.get("sender").toString());
        String threadID = data.get("threadid").toString();

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
    }

    public void clientIDApprovalReply(JSONObject data) {
        // non leader processes client ID approval request reply received
        int approved = Boolean.parseBoolean(data.get("approved").toString()) ? 1 : 0;
        Long threadID = Long.parseLong(data.get("threadid").toString());

        ClientThreadHandler clientThreadHandler = CurrentServer.getInstance()
                .getClientHandlerThread(threadID);
        clientThreadHandler.setApprovedClientID(approved);
        Object lock = clientThreadHandler.getLock();
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    public void roomCreateApprovalRequest(JSONObject data) {
        // leader processes room create approval request received
        String clientID = data.get("clientid").toString();
        String roomID = data.get("roomid").toString();
        int sender = Integer.parseInt(data.get("sender").toString());
        String threadID = data.get("threadid").toString();

        boolean approved = !LeaderServices.getInstance().isRoomCreated(roomID);

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
    }

    public void roomCreateApprovalReply(JSONObject data) {
        // non leader processes room create approval request reply received
        int approved = Boolean.parseBoolean(data.get("approved").toString()) ? 1 : 0;
        Long threadID = Long.parseLong(data.get("threadid").toString());

        ClientThreadHandler clientThreadHandler = CurrentServer.getInstance()
                .getClientHandlerThread(threadID);
        clientThreadHandler.setApprovedRoomCreation(approved);
        Object lock = clientThreadHandler.getLock();
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    public void joinRoomApprovalRequest(JSONObject data) {
        // leader processes join room approval request received

        //get params
        String clientID = data.get("clientid").toString();
        String roomID = data.get("roomid").toString();
        String formerRoomID = data.get("former").toString();
        int sender = Integer.parseInt(data.get("sender").toString());
        String threadID = data.get("threadid").toString();
        boolean isLocalRoomChange = Boolean.parseBoolean(data.get("isLocalRoomChange").toString());

        if (isLocalRoomChange) {
            //local change update leader
            Client client = new Client(clientID, roomID, null);
            LeaderServices.getInstance().localJoinRoomClient(client, formerRoomID);
        }
        else {
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
    }

    public void joinRoomApprovalReply(JSONObject data) {
        // non leader processes room create approval request reply received
        int approved = Boolean.parseBoolean(data.get("approved").toString()) ? 1 : 0;
        Long threadID = Long.parseLong(data.get("threadid").toString());
        String host = data.get("host").toString();
        String port = data.get("port").toString();

        ClientThreadHandler clientThreadHandler = CurrentServer.getInstance()
                .getClientHandlerThread(threadID);

        Object lock = clientThreadHandler.getLock();

        synchronized (lock) {
            clientThreadHandler.setApprovedJoinRoom(approved);
            clientThreadHandler.setApprovedJoinRoomServerHostAddress(host);
            clientThreadHandler.setApprovedJoinRoomServerPort(port);
            lock.notifyAll();
        }
    }

    public void moveJoinAck(JSONObject data) {
        //leader process move join acknowledgement from the target room server after change

        //parse params
        String clientID = data.get("clientid").toString();
        String roomID = data.get("roomid").toString();
        String formerRoomID = data.get("former").toString();
        int sender = Integer.parseInt(data.get("sender").toString());
        String threadID = data.get("threadid").toString();

        Client client = new Client(clientID, roomID, null);
        LeaderServices.getInstance().addClient(client);

        System.out.println("INFO : Moved Client [" + clientID + "] to server s" + sender
                + " and room [" + roomID + "] is updated as current room");
    }

    public void listRequest(JSONObject data) {
        try {
            //leader process list request

            //parse params
            String clientID = data.get("clientid").toString();
            String threadID = data.get("threadid").toString();
            int sender = Integer.parseInt(data.get("sender").toString());

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

    public void listResponse(JSONObject data) {
        Long threadID = Long.parseLong(data.get("threadid").toString());
        JSONArray roomsJSONArray = (JSONArray) data.get("rooms");
        ArrayList<String> roomIDList = new ArrayList(roomsJSONArray);

        ClientThreadHandler clientThreadHandler = CurrentServer.getInstance()
                .getClientHandlerThread(threadID);

        Object lock = clientThreadHandler.getLock();

        synchronized (lock) {
            clientThreadHandler.setRoomsList(roomIDList);
            lock.notifyAll();
        }
    }

    public void deleteRequest(JSONObject data){
        String ownerID = data.get("owner").toString();
        String roomID = data.get("roomid").toString();
        String mainHallID = data.get("mainhall").toString();

        LeaderServices.getInstance().removeRoom(roomID, mainHallID, ownerID);
    }

    public void quit(JSONObject data) {
        String clientID = data.get("clientid").toString();
        String formerRoomID = data.get("former").toString();
        // leader removes client from global room list
        LeaderServices.getInstance().removeClient(clientID, formerRoomID);
        System.out.println("INFO : Client '" + clientID + "' deleted by leader");
    }

    public void leaderStateUpdate(JSONObject data) {
        if(LeaderServices.getInstance().isLeaderElectedAndIamLeader()) {
            if(!leaderStateUpdate.isAlive())
            {
                leaderStateUpdate = new LeaderStateUpdate();
                leaderStateUpdate.start();
            }
            leaderStateUpdate.receiveUpdate(data);
        }
    }

    public void leaderStateUpdateComplete(JSONObject data) {
        int serverID = Integer.parseInt(data.get("serverid").toString());
        if(LeaderServices.getInstance().isLeaderElectedAndMessageFromLeader(serverID)) {
            System.out.println("INFO : Received leader update complete message from s"+serverID);
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
                        case "clientidapprovalrequest" -> clientIDApprovalRequest(serverInputData);

                        case "clientidapprovalreply" -> clientIDApprovalReply(serverInputData);

                        case "roomcreateapprovalrequest" -> roomCreateApprovalRequest(serverInputData);

                        case "roomcreateapprovalreply" -> roomCreateApprovalReply(serverInputData);

                        case "joinroomapprovalrequest" -> joinRoomApprovalRequest(serverInputData);

                        case "joinroomapprovalreply" -> joinRoomApprovalReply(serverInputData);

                        case "movejoinack" -> moveJoinAck(serverInputData);

                        case "listrequest" -> listRequest(serverInputData);

                        case "listresponse" -> listResponse(serverInputData);

                        case "deleterequest" -> deleteRequest(serverInputData);

                        case "quit" -> quit(serverInputData);

                        case "leaderstateupdate" -> leaderStateUpdate(serverInputData);

                        case "leaderstateupdatecomplete" -> leaderStateUpdateComplete(serverInputData);

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
