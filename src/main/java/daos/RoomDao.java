package daos;

import models.Room;

import java.util.HashMap;

public class RoomDao {

    private final HashMap<String, Room> chatRooms = new HashMap<>(); // <roomID, room obj>

    private static RoomDao instance;

    public static RoomDao getInstance() {
        if (instance == null) {
            synchronized (RoomDao.class) {
                if (instance == null) {
                    instance = new RoomDao();
                }
            }
        }
        return instance;
    }

    public HashMap<String, Room> getChatRooms() {
        return chatRooms;
    }
}
