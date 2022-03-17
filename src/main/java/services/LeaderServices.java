package services; //LeaderState

import daos.ClientDao;
import daos.RoomDao;
import models.Client;
import consensus.FastBully;
import models.Room;
import models.CurrentServer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LeaderServices {
    private Integer leaderID;
    private ClientDao clientDao;
    private RoomDao roomDao;

    private LeaderServices() {
        clientDao = ClientDao.getInstance();
        roomDao = RoomDao.getInstance();
    }

    private static LeaderServices leaderServicesInstance;

    public static LeaderServices getInstance() {
        if (leaderServicesInstance == null) {
            synchronized (LeaderServices.class) {
                if (leaderServicesInstance == null) {
                    leaderServicesInstance = new LeaderServices();
                }
            }
        }
        return leaderServicesInstance;
    }

    public boolean isLeader() {
        return CurrentServer.getInstance().getSelfID() == LeaderServices.getInstance().getLeaderID();
    }

    public boolean isLeaderElected() {
        return FastBully.leaderFlag && FastBully.leaderUpdateComplete;
    }

    public boolean isLeaderElectedAndIamLeader() {
        return (FastBully.leaderFlag && CurrentServer.getInstance().getSelfID() == LeaderServices.getInstance().getLeaderID());
    }

    public boolean isLeaderElectedAndMessageFromLeader(int serverID) {
        return (FastBully.leaderFlag && serverID == LeaderServices.getInstance().getLeaderID());
    }

    public boolean isClientRegistered(String clientID) {
        return clientDao.getClients().contains(clientID);
    }

    public void resetLeader() {
        clientDao.getClients().clear();
        roomDao.getChatRooms().clear();
    }

    public void addClient(Client client) {
        clientDao.getClients().add(client.getClientID());
        roomDao.getChatRooms().get(client.getRoomID()).addParticipants(client);
    }

    public void addClientLeaderUpdate(String clientID) {
        clientDao.getClients().add(clientID);
    }

    public void removeClient(String clientID, String formerRoomID) {
        clientDao.getClients().remove(clientID);
        roomDao.getChatRooms().get(formerRoomID).removeParticipants(clientID);
    }

    public void localJoinRoomClient(Client client, String formerRoomID) {
        removeClient(client.getClientID(), formerRoomID);
        addClient(client);
    }

    public boolean isRoomCreated(String roomID) {
        return roomDao.getChatRooms().containsKey(roomID);
    }

    public void addApprovedRoom(Room room) {
        roomDao.getChatRooms().put(room.getRoomID(), room);

        //add client to the new room
        Client client = new Client(room.getOwnerIdentity(), room.getRoomID(), null);
        client.setRoomOwner(true);
        room.addParticipants(client);
    }

    public void addApprovedRoom(String clientID, String roomID, int serverID) {
        Room room = new Room(clientID, roomID, serverID);
        roomDao.getChatRooms().put(roomID, room);

        //add client to the new room
        Client client = new Client(clientID, roomID, null);
        client.setRoomOwner(true);
        room.addParticipants(client);
    }

    public void removeRoom(String roomID, String mainHallID, String ownerID) {
        ConcurrentHashMap<String, Client> formerClientStateMap = roomDao.getChatRooms().get(roomID).getParticipantsMap();
        Room mainHall = roomDao.getChatRooms().get(mainHallID);

        //update client room to main hall , add clients to main hall
        formerClientStateMap.forEach((clientID, client) -> {
            client.setRoomID(mainHallID);
            mainHall.getParticipantsMap().put(client.getClientID(), client);
        });

        //set to room owner false, remove room from map
        formerClientStateMap.get(ownerID).setRoomOwner(false);
        roomDao.getChatRooms().remove(roomID);
    }

    public void addServerDefaultMainHalls(){
        CurrentServer.getInstance().getServers()
                .forEach((serverID, server) -> {
                    String roomID = CurrentServer.getMainHallIDbyServerInt(serverID);
                    roomDao.getChatRooms().put(roomID, new Room("", roomID, serverID));
                });
    }

    public void removeApprovedRoom(String roomID) {
        //TODO : move clients already in room (update server state) on delete
        roomDao.getChatRooms().remove( roomID );
    }


    public int getServerIdIfRoomExist(String roomID) {
        if (roomDao.getChatRooms().containsKey(roomID)) {
            Room targetRoom = roomDao.getChatRooms().get(roomID);
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
        return new ArrayList<>(roomDao.getChatRooms().keySet());
    }

    public List<String> getClientIDList() {
        return clientDao.getClients();
    }

    //remove all rooms and clients by server ID
    public void removeRemoteChatRoomsClientsByServerId(Integer serverId) {
        for (String entry : roomDao.getChatRooms().keySet()) {
            Room remoteRoom = roomDao.getChatRooms().get(entry);
            if(remoteRoom.getServerID()==serverId){
                for(String client : remoteRoom.getParticipantsMap().keySet()){
                    clientDao.getClients().remove(client);
                }
                roomDao.getChatRooms().remove(entry);
            }
        }

    }

}
