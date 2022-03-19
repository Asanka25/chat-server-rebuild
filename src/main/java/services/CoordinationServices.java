package services;

import models.Server;
import org.json.simple.JSONObject;
import models.CurrentServer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class CoordinationServices {

    //send message to server
    public static void sendServer( JSONObject obj, Server destServer) throws IOException
    {
        Socket socket = new Socket(destServer.getServerAddress(),
                destServer.getCoordinationPort());
        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        dataOutputStream.write((obj.toJSONString() + "\n").getBytes( StandardCharsets.UTF_8));
        dataOutputStream.flush();
    }

    //send broadcast message
    public static void sendServerBroadcast(JSONObject obj, ArrayList<Server> serverList) throws IOException {
        for (Server each : serverList) {
            Socket socket = new Socket(each.getServerAddress(), each.getCoordinationPort());
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.write((obj.toJSONString() + "\n").getBytes( StandardCharsets.UTF_8));
            dataOutputStream.flush();
        }
    }
}
