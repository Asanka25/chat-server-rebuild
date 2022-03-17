package handlers; //ClientHandlerThread

import messaging.ClientMessage;
import messaging.ClientMessageContext;
import messaging.ClientMessageContext.CLIENT_MSG_TYPE;
import messaging.ServerMessage;
import models.Client;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import models.Room;
import models.Server;
import models.CurrentServer;
import services.LeaderServices;
import util.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static services.CoordinationServices.*;
import static services.CoordinationServices.send;

public class ClientThreadHandler extends Thread{

    private final Socket clientSocket;
    private Client client;
    private int approvedClientID = -1;
    private int approvedRoomCreation = -1;
    private int approvedJoinRoom = -1;

    private String approvedJoinRoomServerHostAddress;
    private String approvedJoinRoomServerPort;

    private List<String> roomsList;

    final Object lock;

    private boolean quitFlag = false;

    public ClientThreadHandler(Socket clientSocket) {
        String serverID = CurrentServer.getInstance().getServerID();
        this.clientSocket = clientSocket;
        this.lock = new Object();
    }

    public String getClientId()
    {
        return client.getClientID();
    }

    public void setApprovedClientID( int approvedClientID ) {
        this.approvedClientID = approvedClientID;
    }

    public void setApprovedRoomCreation( int approvedRoomCreation ) {
        this.approvedRoomCreation = approvedRoomCreation;
    }

    public void setApprovedJoinRoom(int approvedJoinRoom) {
        this.approvedJoinRoom = approvedJoinRoom;
    }

    public void setApprovedJoinRoomServerHostAddress(String approvedJoinRoomServerHostAddress) {
        this.approvedJoinRoomServerHostAddress = approvedJoinRoomServerHostAddress;
    }

    public void setApprovedJoinRoomServerPort(String approvedJoinRoomServerPort) {
        this.approvedJoinRoomServerPort = approvedJoinRoomServerPort;
    }

    public void setRoomsList(List<String> roomsList) {
        this.roomsList = roomsList;
    }

    public Object getLock() {
        return lock;
    }

    //format message before sending it to client
    private void messageSend(ArrayList<Socket> socketList, ClientMessageContext msgCtx) throws IOException {
        JSONObject sendToClient = new JSONObject();
        if (msgCtx.messageType.equals(ClientMessageContext.CLIENT_MSG_TYPE.NEW_ID)) {
            sendToClient = ClientMessage.getApprovalNewID(msgCtx.isNewClientIdApproved);
            sendClient(sendToClient,clientSocket);
        } else if (msgCtx.messageType.equals(CLIENT_MSG_TYPE.JOIN_ROOM)) {
            sendToClient = ClientMessage.getJoinRoom(msgCtx.clientID, msgCtx.formerRoomID, msgCtx.roomID);
            if (socketList != null) sendBroadcast(sendToClient, socketList);
        } else if (msgCtx.messageType.equals(CLIENT_MSG_TYPE.ROUTE)) {
            sendToClient = ClientMessage.getRoute(msgCtx.roomID, msgCtx.targetHost, msgCtx.targetPort);
            sendClient(sendToClient, clientSocket);
        } else if (msgCtx.messageType.equals(CLIENT_MSG_TYPE.SERVER_CHANGE)) {
            sendToClient = ClientMessage.getServerChange(msgCtx.isServerChangeApproved, msgCtx.approvedServerID);
            sendClient(sendToClient, clientSocket);
            //TODO: do the coherent functions like room change broadcast in same line
            //if (socketList != null) sendBroadcast(sendToClient, socketList);
        }
        else if (msgCtx.messageType.equals(CLIENT_MSG_TYPE.CREATE_ROOM)) {
            sendToClient = ClientMessage.getCreateRoom(msgCtx.roomID, msgCtx.isNewRoomIdApproved);
            sendClient(sendToClient,clientSocket);
        } else if (msgCtx.messageType.equals(CLIENT_MSG_TYPE.BROADCAST_JOIN_ROOM)) {
            sendToClient = ClientMessage.getCreateRoomChange(msgCtx.clientID, msgCtx.formerRoomID, msgCtx.roomID);
            sendBroadcast(sendToClient, socketList);
        } else if (msgCtx.messageType.equals(CLIENT_MSG_TYPE.WHO)) {
            sendToClient = ClientMessage.getWho(msgCtx.roomID, msgCtx.participantsList, msgCtx.clientID);//owner
            sendClient(sendToClient,clientSocket);
        } else if (msgCtx.messageType.equals(CLIENT_MSG_TYPE.LIST)) {
            sendToClient = ClientMessage.getList(msgCtx.roomsList);
            sendClient(sendToClient,clientSocket);
        } else if (msgCtx.messageType.equals(CLIENT_MSG_TYPE.DELETE_ROOM)) {
            sendToClient = ClientMessage.getDeleteRoom(msgCtx.roomID, msgCtx.isDeleteRoomApproved);
            sendClient(sendToClient,clientSocket);
        } else if (msgCtx.messageType.equals(CLIENT_MSG_TYPE.MESSAGE)) {
            sendToClient = ClientMessage.getMessage(msgCtx.clientID,msgCtx.body);
            sendBroadcast(sendToClient, socketList);
        }
    }

    //new identity
    private void newIdentity(String clientID) {
        try {
            if (Utils.isValidIdentity(clientID)) {
                // busy wait until leader is elected
                while (!LeaderServices.getInstance().isLeaderElected()) {
                    Thread.sleep(1000);
                }

                // if self is leader get direct approval
                if (LeaderServices.getInstance().isLeader()) {
                    if (LeaderServices.getInstance().isClientRegistered(clientID)) {
                        approvedClientID = 0;
                        System.out.println("Client is not approved");
                    } else {
                        approvedClientID = 1;
                        System.out.println("Client is approved");
                    }
                } else {
                    try {
                        // send client id approval request to leader
                        JSONObject requestMessage = new JSONObject();
                        requestMessage.put("type", "clientidapprovalrequest");
                        requestMessage.put("clientid", clientID);
                        requestMessage.put("sender", String.valueOf(CurrentServer.getInstance().getSelfID()));
                        requestMessage.put("threadid", String.valueOf(this.getId()));

                        sendToLeader(requestMessage); //todo: check leader clientidapprovalrequest in server handler later

                        System.out.println("Client ID '" + clientID + "' sent to leader for approval");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    synchronized (lock) {
                        while (approvedClientID == -1) {
                            lock.wait(7000);
                        }
                    }
                }

                if (approvedClientID == 1) {
                    this.client = new Client(clientID, CurrentServer.getInstance().getMainHall().getRoomID(), clientSocket);
                    CurrentServer.getInstance().getMainHall().getParticipantsMap().put(clientID, client);

                    //update if self is leader
                    if (LeaderServices.getInstance().isLeader()) {
                        LeaderServices.getInstance().addClient(new Client(clientID, client.getRoomID(), null));
                    }

                    // create broadcast list
                    String mainHallRoomID = CurrentServer.getInstance().getMainHall().getRoomID();

                    JSONObject newIdentityMessage = new JSONObject();
                    newIdentityMessage.put("type", "newidentity");
                    newIdentityMessage.put("approved", "true");

                    JSONObject joinRoomMessage = new JSONObject();
                    joinRoomMessage.put("type", "roomchange");
                    joinRoomMessage.put("identity", clientID);
                    joinRoomMessage.put("former", "");
                    joinRoomMessage.put("roomid", mainHallRoomID);

                    synchronized (clientSocket) {
                        send(newIdentityMessage, clientSocket);
                        CurrentServer.getInstance().getRoomMap().get(mainHallRoomID).getParticipantsMap().forEach((k, v) -> {
                            try {
                                send(joinRoomMessage, v.getSocket());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            ;
                        });
                    }
                } else if (approvedClientID == 0) {
                    JSONObject newIdentityMessage = new JSONObject();
                    newIdentityMessage.put("type", "newidentity");
                    newIdentityMessage.put("approved", "false");

                    send(newIdentityMessage, clientSocket);
                    System.out.println("Already used ClientID");
                }
                approvedClientID = -1;
            }
            else {
                JSONObject newIdentityMessage = new JSONObject();
                newIdentityMessage.put("type", "newidentity");
                newIdentityMessage.put("approved", "false");

                send(newIdentityMessage, clientSocket);
                System.out.println("Wrong ClientID");
            }
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    //list
    private void list() {
        try {
            //reset roomsList
            roomsList = null;

            while (!LeaderServices.getInstance().isLeaderElected()) {
                Thread.sleep(1000);
            }

            // if self is leader get list direct from leader state
            if (LeaderServices.getInstance().isLeader()) {
                roomsList = LeaderServices.getInstance().getRoomIDList();
            } else { // send list request to leader
                JSONObject request = new JSONObject();
                request.put("type", "listrequest");
                request.put("sender", CurrentServer.getInstance().getSelfID());
                request.put("clientid", client.getClientID());
                request.put("threadid", this.getId());

                sendToLeader(request);

                synchronized (lock) {
                    while (roomsList == null) {
                        lock.wait(7000);
                    }
                }
            }

            if (roomsList != null) {
                JSONObject message = new JSONObject();
                message.put("type", "roomlist");
                message.put("rooms", roomsList);

                send(message, clientSocket);
            }
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    //who
    private void who() {
        try {
            String roomID = client.getRoomID();
            Room room = CurrentServer.getInstance().getRoomMap().get(roomID);
            JSONArray participants = new JSONArray();
            participants.addAll(room.getParticipantsMap().keySet());
            String ownerID = room.getOwnerIdentity();

            System.out.println("show participants in room " + roomID);

            JSONObject message = new JSONObject();
            message.put("type", "roomcontents");
            message.put("roomid", roomID);
            message.put("identities", participants);
            message.put("owner", ownerID);

            send(message, clientSocket);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    //create room
    private void createRoom(String newRoomID) {
        try {
            if (!Utils.isValidIdentity(newRoomID)) {
                JSONObject roomCreateMessage = new JSONObject();
                roomCreateMessage.put("type", "createroom");
                roomCreateMessage.put("roomid", newRoomID);
                roomCreateMessage.put("approved", "false");

                send(roomCreateMessage, clientSocket);
                System.out.println("Wrong RoomID");
            }
            else if (client.isRoomOwner()) {
                JSONObject roomCreateMessage = new JSONObject();
                roomCreateMessage.put("type", "createroom");
                roomCreateMessage.put("roomid", newRoomID);
                roomCreateMessage.put("approved", "false");

                send(roomCreateMessage, clientSocket);
                System.out.println("Client already owns a room");
            }
            else {
                // busy wait until leader is elected
                while (!LeaderServices.getInstance().isLeaderElected()) {
                    Thread.sleep(1000);
                }
                // if self is leader get direct approval
                if (LeaderServices.getInstance().isLeader()) {
                    if (LeaderServices.getInstance().isRoomCreated(newRoomID)) {
                        approvedRoomCreation = 0;
                        System.out.println("Room creation is not approved");
                    } else {
                        approvedRoomCreation = 1;
                        System.out.println("Room creation is approved");
                    }
                } else {
                    try {
                        // send room creation approval request to leader
                        JSONObject requestMessage = new JSONObject();
                        requestMessage.put("type", "roomcreateapprovalrequest");
                        requestMessage.put("clientid", client.getClientID());
                        requestMessage.put("roomid", newRoomID);
                        requestMessage.put("sender", String.valueOf(CurrentServer.getInstance().getSelfID()));
                        requestMessage.put("threadid", String.valueOf(this.getId()));


                        sendToLeader(requestMessage); //todo: check leader roomcreateapprovalrequest in server handler later

                        System.out.println("Room ID '" + newRoomID + "' sent to leader for room creation approval");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    synchronized (lock) {
                        while (approvedRoomCreation == -1) {
                            lock.wait(7000);
                        }
                    }
                }

                if (approvedRoomCreation == 1) {

                    String formerRoomID = client.getRoomID();

                    // create broadcast list
                    ArrayList<Socket> formerSocket = new ArrayList<>();
                    CurrentServer.getInstance().getRoomMap().get(formerRoomID).getParticipantsMap().forEach((k, v) -> {
                        formerSocket.add(v.getSocket());
                    });

                    //update server state
                    CurrentServer.getInstance().getRoomMap().get(formerRoomID).removeParticipants(client.getClientID());

                    Room newRoom = new Room(client.getClientID(), newRoomID, CurrentServer.getInstance().getSelfID());
                    CurrentServer.getInstance().getRoomMap().put(newRoomID, newRoom);

                    client.setRoomID(newRoomID);
                    client.setRoomOwner(true);
                    newRoom.addParticipants(client);

                    //update Leader state if self is leader
                    if (LeaderServices.getInstance().isLeader()) {
                        LeaderServices.getInstance().addApprovedRoom(
                                client.getClientID(), newRoomID, CurrentServer.getInstance().getSelfID());
                    }

                    JSONObject roomCreationMessage = new JSONObject();
                    roomCreationMessage.put("type", "createroom");
                    roomCreationMessage.put("roomid", newRoomID);
                    roomCreationMessage.put("approved", "true");

                    JSONObject broadcastMessage = new JSONObject();
                    broadcastMessage.put("type", "roomchange");
                    broadcastMessage.put("identity", client.getClientID());
                    broadcastMessage.put("former", formerRoomID);
                    broadcastMessage.put("roomid", newRoomID);

                    synchronized (clientSocket) { //TODO : check sync | lock on out buffer?
                        send(roomCreationMessage, clientSocket);
                        formerSocket.forEach((v) -> {
                            try {
                                send(broadcastMessage, v);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            ;
                        });
                    }
                } else if (approvedRoomCreation == 0) {
                    JSONObject roomCreationMessage = new JSONObject();
                    roomCreationMessage.put("type", "createroom");
                    roomCreationMessage.put("roomid", newRoomID);
                    roomCreationMessage.put("approved", "false");

                    send(roomCreationMessage, clientSocket);

                    System.out.println("Already used roomID");
                }
                approvedRoomCreation = -1;
            }
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    //join room
    private void joinRoom(String roomID) throws IOException, InterruptedException {
        String formerRoomID = client.getRoomID();

        if (client.isRoomOwner()) { //already owns a room
            ClientMessageContext msgCtx = new ClientMessageContext()
                    .setClientID(client.getClientID())
                    .setRoomID(formerRoomID)       //same
                    .setFormerRoomID(formerRoomID);//same

            System.out.println("WARN : Join room denied, Client" + client.getClientID() + " Owns a room");
            messageSend(null, msgCtx.setMessageType(CLIENT_MSG_TYPE.JOIN_ROOM));

        } else if (CurrentServer.getInstance().getRoomMap().containsKey(roomID)) { //local room change
            //TODO : check sync
            client.setRoomID(roomID);
            CurrentServer.getInstance().getRoomMap().get(formerRoomID).removeParticipants(client.getClientID());
            CurrentServer.getInstance().getRoomMap().get(roomID).addParticipants(client);

            System.out.println("INFO : client [" + client.getClientID() + "] joined room :" + roomID);

            //create broadcast list
            ConcurrentHashMap<String, Client> clientListNew = CurrentServer.getInstance().getRoomMap().get(roomID).getParticipantsMap();
            ConcurrentHashMap<String, Client> clientListOld = CurrentServer.getInstance().getRoomMap().get(formerRoomID).getParticipantsMap();
            HashMap<String, Client> clientList = new HashMap<>();
            clientList.putAll(clientListOld);
            clientList.putAll(clientListNew);

            ArrayList<Socket> SocketList = new ArrayList<>();
            for (String each : clientList.keySet()) {
                SocketList.add(clientList.get(each).getSocket());
            }

            ClientMessageContext msgCtx = new ClientMessageContext()
                    .setClientID(client.getClientID())
                    .setRoomID(roomID)
                    .setFormerRoomID(formerRoomID);

            messageSend(SocketList,  msgCtx.setMessageType(CLIENT_MSG_TYPE.BROADCAST_JOIN_ROOM));

            while (!LeaderServices.getInstance().isLeaderElected()) {
                Thread.sleep(1000);
            }

            // if self is leader update leader state directly
            if (LeaderServices.getInstance().isLeader()) {
                LeaderServices.getInstance().localJoinRoomClient(client, formerRoomID);
            } else {
                //update leader server
                sendToLeader(
                        ServerMessage.getJoinRoomRequest(
                                client.getClientID(),
                                roomID,
                                formerRoomID,
                                String.valueOf(CurrentServer.getInstance().getSelfID()),
                                String.valueOf(this.getId()),
                                String.valueOf(true)
                        )
                );
            }

        } else { //global room change

            while (!LeaderServices.getInstance().isLeaderElected()) {
                Thread.sleep(1000);
            }

            //reset flag
            approvedJoinRoom = -1;
            //check if room id exist and if init route
            if (LeaderServices.getInstance().isLeader()) {
                int serverIDofTargetRoom = LeaderServices.getInstance().getServerIdIfRoomExist(roomID);

                approvedJoinRoom = serverIDofTargetRoom != -1 ? 1 : 0;

                if (approvedJoinRoom == 1) {
                    Server serverOfTargetRoom = CurrentServer.getInstance().getServers().get(serverIDofTargetRoom);
                    approvedJoinRoomServerHostAddress = serverOfTargetRoom.getServerAddress();
                    approvedJoinRoomServerPort = String.valueOf(serverOfTargetRoom.getClientsPort());
                }

                System.out.println("INFO : Received response for route request for join room (Self is Leader)");

            } else {
                sendToLeader(
                        ServerMessage.getJoinRoomRequest(
                                client.getClientID(),
                                roomID,
                                formerRoomID,
                                String.valueOf(CurrentServer.getInstance().getSelfID()),
                                String.valueOf(this.getId()),
                                String.valueOf(false)
                        )
                );

                synchronized (lock) {
                    while (approvedJoinRoom == -1) {
                        System.out.println("INFO : Wait until server approve route on Join room request");
                        lock.wait(7000);
                        //wait for response
                    }
                }

                System.out.println("INFO : Received response for route request for join room");
            }

            if (approvedJoinRoom == 1) {

                //broadcast to former room
                CurrentServer.getInstance().removeClient(client.getClientID(), formerRoomID, getId());
                System.out.println("INFO : client [" + client.getClientID() + "] left room :" + formerRoomID);

                //create broadcast list
                ConcurrentHashMap<String, Client> clientListOld = CurrentServer.getInstance().getRoomMap().get(formerRoomID).getParticipantsMap();
                System.out.println("INFO : Send broadcast to former room in local server");

                ArrayList<Socket> SocketList = new ArrayList<>();
                for (String each : clientListOld.keySet()) {
                    SocketList.add(clientListOld.get(each).getSocket());
                }

                ClientMessageContext msgCtx = new ClientMessageContext()
                        .setClientID(client.getClientID())
                        .setRoomID(roomID)
                        .setFormerRoomID(formerRoomID)
                        .setTargetHost(approvedJoinRoomServerHostAddress)
                        .setTargetPort(approvedJoinRoomServerPort);

                messageSend(SocketList,  msgCtx.setMessageType(CLIENT_MSG_TYPE.BROADCAST_JOIN_ROOM));

                //server change : route
                messageSend(SocketList,  msgCtx.setMessageType(CLIENT_MSG_TYPE.ROUTE));
                System.out.println("INFO : Route Message Sent to Client");
                quitFlag = true;

            } else if (approvedJoinRoom == 0) { // Room not found on system
                ClientMessageContext msgCtx = new ClientMessageContext()
                        .setClientID(client.getClientID())
                        .setRoomID(formerRoomID)       //same
                        .setFormerRoomID(formerRoomID);//same

                System.out.println("WARN : Received room ID ["+roomID + "] does not exist");
                messageSend(null, msgCtx.setMessageType(CLIENT_MSG_TYPE.JOIN_ROOM));
            }

            //reset flag
            approvedJoinRoom = -1;
        }
    }

    //Move join
    private void moveJoin(String roomID, String formerRoomID, String clientID) throws IOException, InterruptedException {
        roomID = (CurrentServer.getInstance().getRoomMap().containsKey(roomID))? roomID: CurrentServer.getInstance().getMainHallID();
        this.client = new Client(clientID, roomID, clientSocket);
        CurrentServer.getInstance().getRoomMap().get(roomID).addParticipants(client);

        //create broadcast list
        ConcurrentHashMap<String, Client> clientListNew = CurrentServer.getInstance().getRoomMap().get(roomID).getParticipantsMap();

        ArrayList<Socket> SocketList = new ArrayList<>();
        for (String each : clientListNew.keySet()) {
            SocketList.add(clientListNew.get(each).getSocket());
        }

        ClientMessageContext msgCtx = new ClientMessageContext()
                .setClientID(client.getClientID())
                .setRoomID(roomID)
                .setFormerRoomID(formerRoomID)
                .setIsServerChangeApproved("true")
                .setApprovedServerID(CurrentServer.getInstance().getServerID());

        messageSend(null, msgCtx.setMessageType(CLIENT_MSG_TYPE.SERVER_CHANGE));
        messageSend(SocketList, msgCtx.setMessageType(CLIENT_MSG_TYPE.BROADCAST_JOIN_ROOM));


        //TODO : check sync
        while (!LeaderServices.getInstance().isLeaderElected()) {
            Thread.sleep(1000);
        }

        //if self is leader update leader state directly
        if (LeaderServices.getInstance().isLeader()) {
            Client client = new Client(clientID, roomID, null);
            LeaderServices.getInstance().addClient(client);
        } else {
            //update leader server
            sendToLeader(
                    ServerMessage.getMoveJoinRequest(
                            client.getClientID(),
                            roomID,
                            formerRoomID,
                            String.valueOf(CurrentServer.getInstance().getSelfID()),
                            String.valueOf(this.getId())
                    )
            );
        }

    }

    //Delete room
    private void deleteRoom(String roomID) throws IOException, InterruptedException {

        String mainHallRoomID = CurrentServer.getInstance().getMainHall().getRoomID();

        if (CurrentServer.getInstance().getRoomMap().containsKey(roomID)) {
            //TODO : check sync
            Room room = CurrentServer.getInstance().getRoomMap().get(roomID);
            if (room.getOwnerIdentity().equals(client.getClientID())) {

                // clients in deleted room
                ConcurrentHashMap<String, Client> formerClientList = CurrentServer.getInstance().getRoomMap()
                        .get(roomID).getParticipantsMap();
                // former clients in main hall
                ConcurrentHashMap<String, Client> mainHallClientList = CurrentServer.getInstance().getRoomMap()
                        .get(mainHallRoomID).getParticipantsMap();
                mainHallClientList.putAll(formerClientList);

                ArrayList<Socket> socketList = new ArrayList<>();
                for (String each : mainHallClientList.keySet()){
                    socketList.add(mainHallClientList.get(each).getSocket());
                }

                CurrentServer.getInstance().getRoomMap().remove(roomID);
                client.setRoomOwner( false );

                // broadcast roomchange message to all clients in deleted room and former clients in main hall
                for(String client:formerClientList.keySet()){
                    String clientID = formerClientList.get(client).getClientID();
                    formerClientList.get(client).setRoomID(mainHallRoomID);
                    CurrentServer.getInstance().getRoomMap().get(mainHallRoomID).addParticipants(formerClientList.get(client));

                    ClientMessageContext msgCtx = new ClientMessageContext()
                            .setClientID(clientID)
                            .setRoomID(mainHallRoomID)
                            .setFormerRoomID(roomID);

                    messageSend(socketList, msgCtx.setMessageType(CLIENT_MSG_TYPE.BROADCAST_JOIN_ROOM));
                }

                ClientMessageContext msgCtx = new ClientMessageContext()
                        .setRoomID(roomID)
                        .setIsDeleteRoomApproved("true");

                //TODO : check sync
                while (!LeaderServices.getInstance().isLeaderElected()) {
                    Thread.sleep(1000);
                }

                //if self is leader update leader state directly
                if (LeaderServices.getInstance().isLeader()) {
                    LeaderServices.getInstance().removeRoom(roomID, mainHallRoomID, client.getClientID());
                } else {
                    //update leader server
                    sendToLeader(
                            ServerMessage.getDeleteRoomRequest(client.getClientID(), roomID, mainHallRoomID)
                    );
                }

                System.out.println("INFO : room [" + roomID + "] was deleted by : " + client.getClientID());

            } else {
                ClientMessageContext msgCtx = new ClientMessageContext()
                        .setRoomID(roomID)
                        .setIsDeleteRoomApproved("false");

                messageSend(null, msgCtx.setMessageType(CLIENT_MSG_TYPE.DELETE_ROOM));
                System.out.println("WARN : Requesting client [" + client.getClientID() + "] does not own the room ID [" + roomID + "]");
            }
        } else {
            ClientMessageContext msgCtx = new ClientMessageContext()
                    .setRoomID(roomID)
                    .setIsDeleteRoomApproved("false");

            messageSend(null, msgCtx.setMessageType(CLIENT_MSG_TYPE.DELETE_ROOM));
            System.out.println("WARN : Received room ID [" + roomID + "] does not exist");
        }
    }

    //quit room
    private void quit() throws IOException, InterruptedException {

        //delete room if room owner
        if (client.isRoomOwner()){
            deleteRoom(client.getRoomID());
            System.out.println("INFO : Deleted room before " + client.getClientID() + " quit");
        }

        //send broadcast with empty target room for quit
        ConcurrentHashMap<String, Client> formerClientList = CurrentServer.getInstance().getRoomMap().get(client.getRoomID()).getParticipantsMap();

        ArrayList<Socket> socketList = new ArrayList<>();
        for (String each:formerClientList.keySet()){
            socketList.add(formerClientList.get(each).getSocket());
        }
        ClientMessageContext msgCtx = new ClientMessageContext()
                .setClientID(client.getClientID())
                .setRoomID("")
                .setFormerRoomID(client.getRoomID());
        messageSend(socketList, msgCtx.setMessageType(CLIENT_MSG_TYPE.BROADCAST_JOIN_ROOM));

        //update Local Server
        CurrentServer.getInstance().removeClient(client.getClientID(), client.getRoomID(), getId());

        // Update global list of Leader
        // send quit message to leader if self is not leader
        if( !LeaderServices.getInstance().isLeader() ) {
            sendToLeader(
                    ServerMessage.getQuit(client.getClientID(), client.getRoomID())
            );
        } else {
            // Leader is self , removes client from global list
            LeaderServices.getInstance().removeClient(client.getClientID(), client.getRoomID() );
        }

        if (!clientSocket.isClosed()) clientSocket.close();
        quitFlag = true;
        System.out.println("INFO : " + client.getClientID() + " quit");
    }

    //message
    private void message(String content) throws IOException {
        String clientID = client.getClientID();
        String roomid = client.getRoomID();

        ConcurrentHashMap<String, Client> clientList = CurrentServer.getInstance().getRoomMap().get(roomid).getParticipantsMap();

        //create broadcast list
        ArrayList<Socket> socketsList = new ArrayList<>();
        for (String each:clientList.keySet()){
            if (!clientList.get(each).getClientID().equals(clientID)){
                socketsList.add(clientList.get(each).getSocket());
            }
        }
        ClientMessageContext msgCtx = new ClientMessageContext()
                .setClientID(clientID)
                .setBody(content);

        messageSend(socketsList, msgCtx.setMessageType(CLIENT_MSG_TYPE.MESSAGE));
    }


    @Override
    public void run() {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            while (!quitFlag) {
                try {
                    String clientInputLine = in.readLine();

                    if (clientInputLine==null){
                        continue;
                    }

                    //convert received message to json object
                    JSONParser jsonParser = new JSONParser();
                    JSONObject clientInputData = (JSONObject) jsonParser.parse(clientInputLine);

                    if (clientInputData.containsKey("type")) {
                        switch (clientInputData.get("type").toString()) {
                            //check new identity format
                            case "newidentity" -> newIdentity(clientInputData.get("identity").toString()); //done

                            //check list
                            case "list" ->  list();//done

                            //check who
                            case "who" -> who(); //doing

                            //check create room
                            case "createroom" -> createRoom(clientInputData.get("roomid").toString()); //done

                            //check join room
                            case "joinroom" -> {
                                String roomID = clientInputData.get("roomid").toString();
                                joinRoom(roomID);
                            }

                            //check move join
                            case "movejoin" -> {
                                String roomID = clientInputData.get("roomid").toString();
                                String formerRoomID = clientInputData.get("former").toString();
                                String clientID = clientInputData.get("identity").toString();
                                moveJoin(roomID, formerRoomID, clientID);
                            }

                            //check delete room
                            case "deleteroom" -> {
                                String roomID = clientInputData.get("roomid").toString();
                                deleteRoom(roomID);
                            }

                            //check message
                            case "message" -> {
                                String content = clientInputData.get("content").toString();
                                message(content);
                            }

                            //check quit
                            case "quit" -> {
                                quit();
                            }

                            default -> System.out.println("Invalid input!");
                        }
                    }
                    else {
                        System.out.println("Invalid input!");
                    }

                } catch (ParseException | InterruptedException | SocketException e) {
                    e.printStackTrace();
                    quit();
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
