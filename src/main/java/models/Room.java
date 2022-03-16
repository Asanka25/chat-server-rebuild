package models;

import java.util.concurrent.ConcurrentHashMap;

public class Room {
    private final String ownerID;
    private final String roomID;
    private final int serverID;

    private final ConcurrentHashMap<String, Client> participantsMap = new ConcurrentHashMap();  //<clientID,clientState>

    //TODO : check sync keyword
    public Room(String ownerID, String roomID, int serverID) {
        this.ownerID = ownerID;
        this.roomID = roomID;
        this.serverID = serverID;
    }

    public synchronized String getRoomID() {
        return roomID;
    }

    public synchronized int getServerID() {
        return serverID;
    }

    public ConcurrentHashMap<String, Client> getParticipantsMap() {
        return participantsMap;
    }

    public void addParticipants(Client client) {
        this.participantsMap.put(client.getClientID(), client);
    }

    public synchronized void removeParticipants(String clientID) {
        this.participantsMap.remove(clientID);
    }

    public String getOwnerIdentity() {
        return ownerID;
    }

}
