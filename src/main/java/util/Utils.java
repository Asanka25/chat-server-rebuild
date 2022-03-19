package util;

import models.CurrentServer;
import models.Server;
import org.json.simple.JSONObject;
import services.LeaderServices;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Utils {
    public static boolean isValidIdentity(String identity){
        return Character.toString(identity.charAt(0)).matches("[a-zA-Z]+") && identity.matches("[a-zA-Z0-9]+") && identity.length() >= 3 && identity.length() <= 16;
    }

    //send message
    public static void send(JSONObject data, Socket socket) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.println(data);
    }

    //send message to leader server
    public static void sendLeader(JSONObject data) throws IOException
    {
        Server leaderServer = CurrentServer.getInstance().getServers().get( LeaderServices.getInstance().getLeaderID() );
        Socket socket = new Socket(leaderServer.getServerAddress(), leaderServer.getCoordinationPort());

        send(data, socket);
    }
}
