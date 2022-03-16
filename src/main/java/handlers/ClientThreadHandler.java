package handlers; //ClientHandlerThread

import messaging.ClientMessage;
import messaging.ClientMessageContext;
import messaging.ClientMessageContext.CLIENT_MSG_TYPE;
import messaging.ServerMessage;
import models.Client;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import server.Room;
import models.Server;
import server.ServerState;
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

import static services.CoordinationServices.*;

public class ClientThreadHandler extends Thread{

    private final Socket clientSocket;
    private Client client;
    private int approvedClientID = -1;
    private int approvedRoomCreation = -1;
    private int approvedJoinRoom = -1;

    private String approvedJoinRoomServerHostAddress;
    private String approvedJoinRoomServerPort;

    private List<String> roomsListTemp;

    final Object lock;

    private boolean quitFlag = false;

    public ClientThreadHandler(Socket clientSocket) {
        String serverID = ServerState.getInstance().getServerID();
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

    public void setRoomsListTemp(List<String> roomsListTemp) {
        this.roomsListTemp = roomsListTemp;
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
    private void newIdentity(String clientID) throws IOException, InterruptedException {
        if (Utils.isValidIdentity(clientID)) {
            // busy wait until leader is elected
            while(!LeaderServices.getInstance().isLeaderElected()) {
                Thread.sleep(1000);
            }

            // if self is leader get direct approval
            if (LeaderServices.getInstance().isLeader()) {
                if (LeaderServices.getInstance().isClientRegistered(clientID)){
                    approvedClientID = 0;
                    System.out.println("Client is not approved");
                }
                else {
                    approvedClientID = 1;
                    System.out.println("Client is approved");
                }
            }
            else {
                try {
                    // send client id approval request to leader
                    JSONObject requestMessage = new JSONObject();
                    sendToLeader(
                            ServerMessage.getClientIdApprovalRequest(clientID,
                                    String.valueOf(ServerState.getInstance().getSelfID()),
                                    String.valueOf(this.getId())
                            )
                    );

                    System.out.println("INFO : Client ID '" + clientID + "' sent to leader for approval");
                } catch (IOException e) {
                    e.printStackTrace();
                }

                synchronized (lock) {
                    while (approvedClientID == -1) {
                        lock.wait(7000);
                    }
                }
            }

            if( approvedClientID == 1 ) {
                System.out.println( "INFO : Received correct ID :" + clientID );
                this.client = new Client( clientID, ServerState.getInstance().getMainHall().getRoomID(), clientSocket );
                ServerState.getInstance().getMainHall().addParticipants(client);

                //update if self is leader
                if (LeaderServices.getInstance().isLeader()) {
                    LeaderServices.getInstance().addClient(new Client( clientID, client.getRoomID(), null ));
                }

                // create broadcast list
                String mainHallRoomID = ServerState.getInstance().getMainHall().getRoomID();
                HashMap<String, Client> mainHallClientList = ServerState.getInstance()
                        .getRoomMap()
                        .get( mainHallRoomID )
                        .getClientStateMap();

                ArrayList<Socket> socketList = new ArrayList<>();
                for( String each : mainHallClientList.keySet() )
                {
                    socketList.add( mainHallClientList.get( each ).getSocket() );
                }

                ClientMessageContext msgCtx = new ClientMessageContext()
                        .setMessageType(CLIENT_MSG_TYPE.NEW_ID)
                        .setClientID(clientID)
                        .setIsNewClientIdApproved("true")
                        .setFormerRoomID("")
                        .setRoomID(mainHallRoomID);

                synchronized( clientSocket )
                {
                    messageSend( null, msgCtx );
                    messageSend( socketList, msgCtx.setMessageType(CLIENT_MSG_TYPE.JOIN_ROOM) );
                }
            }
            else if( approvedClientID == 0 ) {
                ClientMessageContext msgCtx = new ClientMessageContext()
                        .setMessageType(CLIENT_MSG_TYPE.NEW_ID)
                        .setClientID(clientID)
                        .setIsNewClientIdApproved("false");

                System.out.println("WARN : ID already in use");
                messageSend(null, msgCtx);
            }
            approvedClientID = -1;
        }
        else {
            ClientMessageContext msgCtx = new ClientMessageContext()
                    .setClientID(clientID)
                    .setIsNewClientIdApproved("false");

            System.out.println("WARN : Recieved wrong ID type");
            messageSend(null, msgCtx.setMessageType(CLIENT_MSG_TYPE.NEW_ID));
        }
    }

    //list
    private void list() throws IOException, InterruptedException {
        //reset Temp room list
        roomsListTemp = null;

        while (!LeaderServices.getInstance().isLeaderElected()) {
            Thread.sleep(1000);
        }

        // if self is leader get list direct from leader state
        if (LeaderServices.getInstance().isLeader()) {
            roomsListTemp = LeaderServices.getInstance().getRoomIDList();
        } else { // send list request to leader
            sendToLeader(
                    ServerMessage.getListRequest(
                            client.getClientID(),
                            String.valueOf(this.getId()),
                            String.valueOf(ServerState.getInstance().getSelfID()))
            );

            synchronized (lock) {
                while (roomsListTemp == null) {
                    lock.wait(7000);
                }
            }
        }

        if (roomsListTemp != null) {
            ClientMessageContext msgCtx = new ClientMessageContext()
                    .setRoomsList(roomsListTemp);

            System.out.println("INFO : Recieved rooms in the system :" + roomsListTemp);
            messageSend(null, msgCtx.setMessageType(CLIENT_MSG_TYPE.LIST));
        }
    }

    //who
    private void who() throws IOException {
        String roomID = client.getRoomID();
        Room room = ServerState.getInstance().getRoomMap().get(roomID);
        HashMap<String, Client> clientStateMap = room.getClientStateMap();
        List<String> participantsList = new ArrayList<>(clientStateMap.keySet());
        String ownerID = room.getOwnerIdentity();

        ClientMessageContext msgCtx = new ClientMessageContext()
                .setClientID(ownerID) //Owner
                .setRoomID(client.getRoomID())
                .setParticipantsList(participantsList);

        System.out.println("LOG  : participants in room [" + roomID + "] : " + participantsList);
        messageSend(null, msgCtx.setMessageType(CLIENT_MSG_TYPE.WHO));
    }

    //create room
    private void createRoom(String newRoomID) throws IOException, InterruptedException
    {
        if (Utils.isValidIdentity(newRoomID) && !client.isRoomOwner()) {
            // busy wait until leader is elected
            while(!LeaderServices.getInstance().isLeaderElected()) {
                Thread.sleep(1000);
            }
            // if self is leader get direct approval
            if (LeaderServices.getInstance().isLeader()) {
                boolean approved = LeaderServices.getInstance().isRoomCreationApproved(newRoomID);
                approvedRoomCreation = approved ? 1 : 0;
                System.out.println("INFO : Room '" + newRoomID +
                        "' creation request from client " + client.getClientID() +
                        " is" + (approved ? " " : " not ") + "approved");
            } else {
                try {
                    // send room creation approval request to leader
                    sendToLeader(
                            ServerMessage.getRoomCreateApprovalRequest(client.getClientID(),
                                    newRoomID,
                                    String.valueOf(ServerState.getInstance().getSelfID()),
                                    String.valueOf(this.getId())
                            )
                    );

                    System.out.println("INFO : Room '" + newRoomID + "' create request by '"
                            + client.getClientID() + "' sent to leader for approval");
                } catch (Exception e) {
                    e.printStackTrace();
                }

                synchronized (lock) {
                    while (approvedRoomCreation == -1) {
                        lock.wait(7000);
                    }
                }
            }

            if( approvedRoomCreation == 1) {
                System.out.println( "INFO : Received correct room ID :" + newRoomID );

                String formerRoomID = client.getRoomID();

                // list of clients inside former room
                HashMap<String, Client> clientList = ServerState.getInstance().getRoomMap().get( formerRoomID ).getClientStateMap();

                // create broadcast list
                ArrayList<Socket> formerSocket = new ArrayList<>();
                for( String each : clientList.keySet() )
                {
                    formerSocket.add( clientList.get( each ).getSocket() );
                }

                //update server state
                ServerState.getInstance().getRoomMap().get( formerRoomID ).removeParticipants( client.getClientID() );

                Room newRoom = new Room( client.getClientID(), newRoomID, ServerState.getInstance().getSelfID() );
                ServerState.getInstance().getRoomMap().put( newRoomID, newRoom );

                client.setRoomID( newRoomID );
                client.setRoomOwner( true );
                newRoom.addParticipants(client);

                //update Leader state if self is leader
                if (LeaderServices.getInstance().isLeader()) {
                    LeaderServices.getInstance().addApprovedRoom(
                            client.getClientID(), newRoomID, ServerState.getInstance().getSelfID());
                }

                synchronized (clientSocket) { //TODO : check sync | lock on out buffer?
                    ClientMessageContext msgCtx = new ClientMessageContext()
                            .setClientID(client.getClientID())
                            .setRoomID(newRoomID)
                            .setFormerRoomID(formerRoomID)
                            .setIsNewRoomIdApproved("true");

                    messageSend(null, msgCtx.setMessageType(CLIENT_MSG_TYPE.CREATE_ROOM));
                    messageSend(formerSocket, msgCtx.setMessageType(CLIENT_MSG_TYPE.BROADCAST_JOIN_ROOM));
                }

            } else if ( approvedRoomCreation == 0 ) {
                ClientMessageContext msgCtx = new ClientMessageContext()
                        .setRoomID(newRoomID)
                        .setIsNewRoomIdApproved("false");

                System.out.println("WARN : Room id [" + newRoomID + "] already in use");
                messageSend(null, msgCtx.setMessageType(CLIENT_MSG_TYPE.CREATE_ROOM));
            }
            approvedRoomCreation = -1;
        } else {
            ClientMessageContext msgCtx = new ClientMessageContext()
                    .setRoomID(newRoomID)
                    .setIsNewRoomIdApproved("false");

            System.out.println("WARN : Received wrong room ID type or client already owns a room [" + newRoomID + "]");
            messageSend(null, msgCtx.setMessageType(CLIENT_MSG_TYPE.CREATE_ROOM));
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

        } else if (ServerState.getInstance().getRoomMap().containsKey(roomID)) { //local room change
            //TODO : check sync
            client.setRoomID(roomID);
            ServerState.getInstance().getRoomMap().get(formerRoomID).removeParticipants(client.getClientID());
            ServerState.getInstance().getRoomMap().get(roomID).addParticipants(client);

            System.out.println("INFO : client [" + client.getClientID() + "] joined room :" + roomID);

            //create broadcast list
            HashMap<String, Client> clientListNew = ServerState.getInstance().getRoomMap().get(roomID).getClientStateMap();
            HashMap<String, Client> clientListOld = ServerState.getInstance().getRoomMap().get(formerRoomID).getClientStateMap();
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
                                String.valueOf(ServerState.getInstance().getSelfID()),
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
                    Server serverOfTargetRoom = ServerState.getInstance().getServers().get(serverIDofTargetRoom);
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
                                String.valueOf(ServerState.getInstance().getSelfID()),
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
                ServerState.getInstance().removeClient(client.getClientID(), formerRoomID, getId());
                System.out.println("INFO : client [" + client.getClientID() + "] left room :" + formerRoomID);

                //create broadcast list
                HashMap<String, Client> clientListOld = ServerState.getInstance().getRoomMap().get(formerRoomID).getClientStateMap();
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
        roomID = (ServerState.getInstance().getRoomMap().containsKey(roomID))? roomID:ServerState.getInstance().getMainHallID();
        this.client = new Client(clientID, roomID, clientSocket);
        ServerState.getInstance().getRoomMap().get(roomID).addParticipants(client);

        //create broadcast list
        HashMap<String, Client> clientListNew = ServerState.getInstance().getRoomMap().get(roomID).getClientStateMap();

        ArrayList<Socket> SocketList = new ArrayList<>();
        for (String each : clientListNew.keySet()) {
            SocketList.add(clientListNew.get(each).getSocket());
        }

        ClientMessageContext msgCtx = new ClientMessageContext()
                .setClientID(client.getClientID())
                .setRoomID(roomID)
                .setFormerRoomID(formerRoomID)
                .setIsServerChangeApproved("true")
                .setApprovedServerID(ServerState.getInstance().getServerID());

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
                            String.valueOf(ServerState.getInstance().getSelfID()),
                            String.valueOf(this.getId())
                    )
            );
        }

    }

    //Delete room
    private void deleteRoom(String roomID) throws IOException, InterruptedException {

        String mainHallRoomID = ServerState.getInstance().getMainHall().getRoomID();

        if (ServerState.getInstance().getRoomMap().containsKey(roomID)) {
            //TODO : check sync
            Room room = ServerState.getInstance().getRoomMap().get(roomID);
            if (room.getOwnerIdentity().equals(client.getClientID())) {

                // clients in deleted room
                HashMap<String, Client> formerClientList = ServerState.getInstance().getRoomMap()
                        .get(roomID).getClientStateMap();
                // former clients in main hall
                HashMap<String, Client> mainHallClientList = ServerState.getInstance().getRoomMap()
                        .get(mainHallRoomID).getClientStateMap();
                mainHallClientList.putAll(formerClientList);

                ArrayList<Socket> socketList = new ArrayList<>();
                for (String each : mainHallClientList.keySet()){
                    socketList.add(mainHallClientList.get(each).getSocket());
                }

                ServerState.getInstance().getRoomMap().remove(roomID);
                client.setRoomOwner( false );

                // broadcast roomchange message to all clients in deleted room and former clients in main hall
                for(String client:formerClientList.keySet()){
                    String clientID = formerClientList.get(client).getClientID();
                    formerClientList.get(client).setRoomID(mainHallRoomID);
                    ServerState.getInstance().getRoomMap().get(mainHallRoomID).addParticipants(formerClientList.get(client));

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
        HashMap<String, Client> formerClientList = ServerState.getInstance().getRoomMap().get(client.getRoomID()).getClientStateMap();

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
        ServerState.getInstance().removeClient(client.getClientID(), client.getRoomID(), getId());

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

        HashMap<String, Client> clientList = ServerState.getInstance().getRoomMap().get(roomid).getClientStateMap();

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
                            case "newidentity" -> {
                                newIdentity(clientInputData.get("identity").toString());
                            }

                            //check create room
                            case "createroom" -> {
                                String newRoomID = clientInputData.get("roomid").toString();
                                createRoom(newRoomID);
                            }

                            //check who
                            case "who" -> {
                                who();
                            }

                            //check list
                            case "list" -> {
                                list();
                            }

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
                    } else {
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
