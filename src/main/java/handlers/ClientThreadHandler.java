package handlers; //ClientHandlerThread

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import static util.Utils.send;
import static util.Utils.sendLeader;

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

    private boolean boolQuit = false;

    public ClientThreadHandler(Socket clientSocket) {
        String serverID = CurrentServer.getInstance().getServerID();
        this.clientSocket = clientSocket;
        this.lock = new Object();
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

                        sendLeader(requestMessage); //todo: check leader clientidapprovalrequest in server handler later

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

                sendLeader(request);

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


                        sendLeader(requestMessage); //todo: check leader roomcreateapprovalrequest in server handler later

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

    private void joinRoom(String roomID) {
        try {
            String formerRoomID = client.getRoomID();

            if (client.isRoomOwner()) {
                JSONObject message = new JSONObject();
                message.put("type", "roomchange");
                message.put("identity", client.getClientID());
                message.put("former", formerRoomID);
                message.put("roomid", formerRoomID);

                send(message, clientSocket);

                System.out.println(client.getClientID() + " Owns a room");
            }
            else if (CurrentServer.getInstance().getRoomMap().containsKey(roomID)) {
                //local room change
                //TODO : check sync
                client.setRoomID(roomID);
                CurrentServer.getInstance().getRoomMap().get(formerRoomID).removeParticipants(client.getClientID());
                CurrentServer.getInstance().getRoomMap().get(roomID).addParticipants(client);

                System.out.println(client.getClientID() + " join to room " + roomID);

                //create broadcast list
                Collection<Client> newRoomClients = CurrentServer.getInstance().getRoomMap().get(roomID).getParticipantsMap().values();
                Collection<Client> formerRoomClients = CurrentServer.getInstance().getRoomMap().get(formerRoomID).getParticipantsMap().values();

                JSONObject broadcastMessage = new JSONObject();
                broadcastMessage.put("type", "roomchange");
                broadcastMessage.put("identity", client.getClientID());
                broadcastMessage.put("former", formerRoomID);
                broadcastMessage.put("roomid", roomID);

                newRoomClients.forEach((i) -> {
                    try {
                        send(broadcastMessage, i.getSocket());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

                formerRoomClients.forEach((i) -> {
                    try {
                        send(broadcastMessage, i.getSocket());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

                while (!LeaderServices.getInstance().isLeaderElected()) {
                    Thread.sleep(1000);
                }

                // if self is leader update leader state directly
                if (LeaderServices.getInstance().isLeader()) {
                    LeaderServices.getInstance().localJoinRoomClient(client, formerRoomID);
                } else {
                    JSONObject request = new JSONObject();
                    request.put("type", "joinroomapprovalrequest");
                    request.put("sender", String.valueOf(CurrentServer.getInstance().getSelfID()));
                    request.put("roomid", roomID);
                    request.put("former", formerRoomID);
                    request.put("clientid", client.getClientID());
                    request.put("threadid", String.valueOf(this.getId()));
                    request.put("isLocalRoomChange", "true");

                    //update leader server
                    sendLeader(request);
                }

            }
            else {
                //global room change

                while (!LeaderServices.getInstance().isLeaderElected()) {
                    Thread.sleep(1000);
                }

                //reset flag
                approvedJoinRoom = -1;
                //check if room id exist and if init route
                if (LeaderServices.getInstance().isLeader()) {
                    int roomServerID = LeaderServices.getInstance().getServerIdIfRoomExist(roomID);

                    if (roomServerID != -1) {
                        approvedJoinRoom = 1;
                        Server roomServer = CurrentServer.getInstance().getServers().get(roomServerID);
                        approvedJoinRoomServerHostAddress = roomServer.getServerAddress();
                        approvedJoinRoomServerPort = String.valueOf(roomServer.getClientsPort());
                    } else {
                        approvedJoinRoom = 0;
                    }

                } else {
                    JSONObject request = new JSONObject();
                    request.put("type", "joinroomapprovalrequest");
                    request.put("sender", String.valueOf(CurrentServer.getInstance().getSelfID()));
                    request.put("roomid", roomID);
                    request.put("former", formerRoomID);
                    request.put("clientid", client.getClientID());
                    request.put("threadid", String.valueOf(this.getId()));
                    request.put("isLocalRoomChange", "false");

                    sendLeader(request);

                    synchronized (lock) {
                        while (approvedJoinRoom == -1) {
                            lock.wait(7000);
                        }
                    }

                    System.out.println("Received response for join room route request");
                }

                if (approvedJoinRoom == 1) {

                    //broadcast to former room
                    CurrentServer.getInstance().removeClient(client.getClientID(), formerRoomID, getId());
                    System.out.println(client.getClientID() + " left " + formerRoomID + " room");

                    Collection<Client> formerRoomClients = CurrentServer.getInstance().getRoomMap().get(formerRoomID).getParticipantsMap().values();

                    JSONObject broadcastMessage = new JSONObject();
                    broadcastMessage.put("type", "roomchange");
                    broadcastMessage.put("identity", client.getClientID());
                    broadcastMessage.put("former", formerRoomID);
                    broadcastMessage.put("roomid", roomID);

                    formerRoomClients.forEach((i) -> {
                        try {
                            send(broadcastMessage, i.getSocket());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });

                    //server change : route
                    JSONObject routeMessage = new JSONObject();
                    routeMessage.put("type", "route");
                    routeMessage.put("roomid", roomID);
                    routeMessage.put("host", approvedJoinRoomServerHostAddress);
                    routeMessage.put("port", approvedJoinRoomServerPort);

                    send(routeMessage, clientSocket);
                    System.out.println("Route Message Sent to Client");
                    boolQuit = true;
                } else if (approvedJoinRoom == 0) {
                    // Room not found on system
                    JSONObject message = new JSONObject();
                    message.put("type", "roomchange");
                    message.put("identity", client.getClientID());
                    message.put("former", formerRoomID);
                    message.put("roomid", formerRoomID);

                    send(message, clientSocket);

                    System.out.println(roomID + "room does not exist");
                }

                //reset flag
                approvedJoinRoom = -1;
            }
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void moveJoin(JSONObject inputData) {
        try {
            String roomID = inputData.get("roomid").toString();
            String formerRoomID = inputData.get("former").toString();
            String clientID = inputData.get("identity").toString();

            if (CurrentServer.getInstance().getRoomMap().containsKey(roomID)) {
                roomID = inputData.get("roomid").toString();
            } else {
                roomID = CurrentServer.getInstance().getMainHallID();
            }

            this.client = new Client(clientID, roomID, clientSocket);
            CurrentServer.getInstance().getRoomMap().get(roomID).addParticipants(client);

            //create broadcast list
            Collection<Client> roomParticipants = CurrentServer.getInstance().getRoomMap().get(roomID).getParticipantsMap().values();

            JSONObject serverChangeMessage = new JSONObject();
            serverChangeMessage.put("type", "serverchange");
            serverChangeMessage.put("approved", "true");
            serverChangeMessage.put("serverid", CurrentServer.getInstance().getServerID());

            send(serverChangeMessage, clientSocket);

            JSONObject broadcastMessage = new JSONObject();
            broadcastMessage.put("type", "roomchange");
            broadcastMessage.put("identity", clientID);
            broadcastMessage.put("former", formerRoomID);
            broadcastMessage.put("roomid", roomID);

            roomParticipants.forEach((i) -> {
                try {
                    send(broadcastMessage, i.getSocket());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            //TODO : check sync
            while (!LeaderServices.getInstance().isLeaderElected()) {
                Thread.sleep(1000);
            }

            //if self is leader update leader state directly
            if (LeaderServices.getInstance().isLeader()) {
                LeaderServices.getInstance().addClient(new Client(clientID, roomID, null));
            } else {
                JSONObject ack = new JSONObject();
                ack.put("type", "movejoinack");
                ack.put("sender", String.valueOf(CurrentServer.getInstance().getSelfID()));
                ack.put("roomid", roomID);
                ack.put("former", formerRoomID);
                ack.put("clientid", client.getClientID());
                ack.put("threadid", String.valueOf(this.getId()));

                //update leader server
                sendLeader(ack);
            }
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void deleteRoom(String roomID) {
        try {
            String mainHallID = CurrentServer.getInstance().getMainHall().getRoomID();

            if (CurrentServer.getInstance().getRoomMap().containsKey(roomID)) {
                //TODO : check sync
                Room room = CurrentServer.getInstance().getRoomMap().get(roomID);
                if (room.getOwnerIdentity().equals(client.getClientID())) {

                    // clients in deleted room
                    ConcurrentHashMap<String, Client> formerClients = CurrentServer.getInstance().getRoomMap().get(roomID).getParticipantsMap();
                    // former clients in main hall
                    Collection<Client> mainHallClients = CurrentServer.getInstance().getRoomMap().get(mainHallID).getParticipantsMap().values();

                    ArrayList<Socket> socketList = new ArrayList<>();

                    formerClients.values().forEach((i) -> {
                        socketList.add(i.getSocket());
                    });

                    mainHallClients.forEach((i) -> {
                        socketList.add(i.getSocket());
                    });

                    CurrentServer.getInstance().getRoomMap().remove(roomID);
                    client.setRoomOwner(false);

                    // broadcast roomchange message to all clients in deleted room and former clients in main hall
                    formerClients.forEach((k, v) -> {
                        v.setRoomID(mainHallID);
                        CurrentServer.getInstance().getRoomMap().get(mainHallID).addParticipants(v);

                        JSONObject message = new JSONObject();
                        message.put("type", "roomchange");
                        message.put("identity", k);
                        message.put("former", roomID);
                        message.put("roomid", mainHallID);

                        socketList.forEach((i) -> {
                            try {
                                send(message, i);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                    });

                    //TODO : check sync
                    while (!LeaderServices.getInstance().isLeaderElected()) {
                        Thread.sleep(1000);
                    }

                    //if self is leader update leader state directly
                    if (LeaderServices.getInstance().isLeader()) {
                        LeaderServices.getInstance().removeRoom(roomID, mainHallID, client.getClientID());
                    } else {
                        //update leader server
                        JSONObject request = new JSONObject();
                        request.put("type", "deleterequest");
                        request.put("owner", client.getClientID());
                        request.put("roomid", roomID);
                        request.put("mainhall", mainHallID);

                        sendLeader(request);
                    }

                    System.out.println(roomID + " room is deleted");

                } else {
                    JSONObject message = new JSONObject();
                    message.put("type", "deleteroom");
                    message.put("roomid", roomID);
                    message.put("approved", "false");

                    send(message, clientSocket);

                    System.out.println("Requesting client is not the owner of the room " + roomID);
                }
            } else {
                JSONObject message = new JSONObject();
                message.put("type", "deleteroom");
                message.put("roomid", roomID);
                message.put("approved", "false");

                send(message, clientSocket);

                System.out.println("Room ID " + roomID + " does not exist");
            }
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void quit(){
        try {
            //delete room if room owner
            if (client.isRoomOwner()) {
                deleteRoom(client.getRoomID());
                System.out.println(client.getRoomID() + " room deleted due to owner quiting");
            }

            JSONObject quitMessage = new JSONObject();
            quitMessage.put("type", "roomchange");
            quitMessage.put("identity", client.getClientID());
            quitMessage.put("former", client.getRoomID());
            quitMessage.put("roomid", "");

            Collection<Client> roomClients = CurrentServer.getInstance().getRoomMap().get(client.getRoomID()).getParticipantsMap().values();

            roomClients.forEach((i) -> {
                try {
                    send(quitMessage, i.getSocket());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            //update Local Server
            CurrentServer.getInstance().removeClient(client.getClientID(), client.getRoomID(), getId());

            // Update global list of Leader
            // send quit message to leader if self is not leader
            if (LeaderServices.getInstance().isLeader()) {
                // Leader is self , removes client from global list
                LeaderServices.getInstance().removeClient(client.getClientID(), client.getRoomID());
            } else {
                JSONObject leaderMessage = new JSONObject();
                leaderMessage.put("type", "quit");
                leaderMessage.put("clientid", client.getClientID());
                leaderMessage.put("former", client.getRoomID());

                sendLeader(leaderMessage);
            }

            if (!clientSocket.isClosed()) {
                clientSocket.close();
            }

            System.out.println(client.getClientID() + " quit");
            boolQuit = true;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void message(String content) {
        String clientID = client.getClientID();
        String roomid = client.getRoomID();

        JSONObject message = new JSONObject();
        message.put("type", "message");
        message.put("identity", clientID);
        message.put("content", content);


        CurrentServer.getInstance().getRoomMap().get(roomid).getParticipantsMap().forEach((k, v) -> {
            if (!k.equals(clientID)) {
                try {
                    send(message, v.getSocket());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void run() {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            while (!boolQuit) {
                String clientInputLine = in.readLine();

                JSONParser jsonParser = new JSONParser();
                JSONObject clientInputData = (JSONObject) jsonParser.parse(clientInputLine);

                switch (clientInputData.get("type").toString()) {
                    case "newidentity" -> newIdentity(clientInputData.get("identity").toString());

                    case "list" -> list();

                    case "who" -> who();

                    case "createroom" -> createRoom(clientInputData.get("roomid").toString());

                    case "joinroom" -> joinRoom(clientInputData.get("roomid").toString());

                    case "movejoin" -> moveJoin(clientInputData);

                    case "deleteroom" -> deleteRoom(clientInputData.get("roomid").toString());

                    case "message" -> message(clientInputData.get("content").toString());

                    case "quit" -> quit();
                }
            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

}
