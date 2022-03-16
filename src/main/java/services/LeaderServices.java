package services; //LeaderState

import models.Client;
import consensus.FastBully;
import server.Room;
import server.ServerState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LeaderServices {
    private Integer leaderID;

    private final List<String> activeClients = new ArrayList<>();
    private final HashMap<String, Room> activeChatRooms = new HashMap<>(); // <roomID, room obj>

    private LeaderServices() {}

    private static LeaderServices leaderServicesInstance;

    public static LeaderServices getInstance() {
        if (leaderServicesInstance == null) {
            synchronized (LeaderServices.class) {
                if (leaderServicesInstance == null) {
                    leaderServicesInstance = new LeaderServices();
//                    leaderStateInstance.addServerDefaultMainHalls();
                }
            }
        }
        return leaderServicesInstance;
    }

    public boolean isLeader() {
        return ServerState.getInstance().getSelfID() == LeaderServices.getInstance().getLeaderID();
    }

    public boolean isLeaderElected() {
        return FastBully.leaderFlag && FastBully.leaderUpdateComplete;
    }

    public boolean isLeaderElectedAndIamLeader() {
        return (FastBully.leaderFlag && ServerState.getInstance().getSelfID() == LeaderServices.getInstance().getLeaderID());
    }

    public boolean isLeaderElectedAndMessageFromLeader(int serverID) {
        return (FastBully.leaderFlag && serverID == LeaderServices.getInstance().getLeaderID());
    }

    public boolean isClientRegistered(String clientID) {
        return activeClients.contains(clientID); //todo: end here
    }

    public void resetLeader() {
        activeClients.clear();
        activeChatRooms.clear();
    }

    public void addClient(Client client) {
        activeClients.add(client.getClientID());
        activeChatRooms.get(client.getRoomID()).addParticipants(client);
    }

    public void addClientLeaderUpdate(String clientID) {
        activeClients.add(clientID);
    }

    public void removeClient(String clientID, String formerRoomID) {
        activeClients.remove(clientID);
        activeChatRooms.get(formerRoomID).removeParticipants(clientID);
    }

    public void localJoinRoomClient(Client client, String formerRoomID) {
        removeClient(client.getClientID(), formerRoomID);
        addClient(client);
    }

    public boolean isRoomCreationApproved( String roomID ) {
        return !(activeChatRooms.containsKey( roomID ));
    }

    public void addApprovedRoom(String clientID, String roomID, int serverID) {
        Room room = new Room(clientID, roomID, serverID);
        activeChatRooms.put(roomID, room);

        //add client to the new room
        Client client = new Client(clientID, roomID, null);
        client.setRoomOwner(true);
        room.addParticipants(client);
    }

    public void removeRoom(String roomID, String mainHallID, String ownerID) {
        HashMap<String, Client> formerClientStateMap = this.activeChatRooms.get(roomID).getClientStateMap();
        Room mainHall = this.activeChatRooms.get(mainHallID);

        //update client room to main hall , add clients to main hall
        formerClientStateMap.forEach((clientID, client) -> {
            client.setRoomID(mainHallID);
            mainHall.getClientStateMap().put(client.getClientID(), client);
        });

        //set to room owner false, remove room from map
        formerClientStateMap.get(ownerID).setRoomOwner(false);
        this.activeChatRooms.remove(roomID);
    }

    public void addServerDefaultMainHalls(){
        ServerState.getInstance().getServers()
                .forEach((serverID, server) -> {
                    String roomID = ServerState.getMainHallIDbyServerInt(serverID);
                    this.activeChatRooms.put(roomID, new Room("", roomID, serverID));
                });
    }

    public void removeApprovedRoom(String roomID) {
        //TODO : move clients already in room (update server state) on delete
        activeChatRooms.remove( roomID );
    }


    public int getServerIdIfRoomExist(String roomID) {
        if (this.activeChatRooms.containsKey(roomID)) {
            Room targetRoom = activeChatRooms.get(roomID);
            return targetRoom.getServerID();
        } else {
            return -1;
        }
    }

    public Integer getLeaderID()
    {
        return leaderID;
    }

    public void setLeaderID( int leaderID )
    {
        this.leaderID = leaderID;
    }

    public ArrayList<String> getRoomIDList() {
        return new ArrayList<>(this.activeChatRooms.keySet());
    }

    public List<String> getClientIDList() {
        return this.activeClients;
    }

    //remove all rooms and clients by server ID
    public void removeRemoteChatRoomsClientsByServerId(Integer serverId) {
        for (String entry : activeChatRooms.keySet()) {
            Room remoteRoom = activeChatRooms.get(entry);
            if(remoteRoom.getServerID()==serverId){
                for(String client : remoteRoom.getClientStateMap().keySet()){
                    activeClients.remove(client);
                }
                activeChatRooms.remove(entry);
            }
        }

    }

}
