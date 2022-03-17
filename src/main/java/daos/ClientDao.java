package daos;

import java.util.ArrayList;
import java.util.List;

public class ClientDao {

    private final List<String> clients;

    private ClientDao() {
        clients = new ArrayList<>();
    }

    private static ClientDao instance;

    public static ClientDao getInstance() {
        if (instance == null) {
            synchronized (ClientDao.class) {
                if (instance == null) {
                    instance = new ClientDao();
                }
            }
        }
        return instance;
    }

    public List<String> getClients() {
        return clients;
    }

}
