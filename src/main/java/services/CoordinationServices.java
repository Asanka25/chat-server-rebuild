package services;

import models.Server;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import models.CurrentServer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class CoordinationServices {
    public static JSONObject convertToJson(String jsonString)
    {
        JSONObject j_object = null;
        try
        {
            JSONParser jsonParser = new JSONParser();
            Object object = jsonParser.parse(jsonString);
            j_object = (JSONObject) object;

        } catch(ParseException e) {
            e.printStackTrace();
        }
        return j_object;
    }

    //send broadcast message
    public static void sendBroadcast(JSONObject obj, ArrayList<Socket> socketList) throws IOException {
        for (Socket each : socketList) {
            Socket TEMP_SOCK = (Socket) each;
            PrintWriter TEMP_OUT = new PrintWriter(TEMP_SOCK.getOutputStream());
            TEMP_OUT.println(obj);
            TEMP_OUT.flush();
        }
    }

    //send message
    public static void send(JSONObject obj, Socket socket) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        dataOutputStream.write((obj.toJSONString() + "\n").getBytes(StandardCharsets.UTF_8));
        dataOutputStream.flush();
    }

    //send message to client
    public static void sendClient(JSONObject obj, Socket socket) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        dataOutputStream.write((obj.toJSONString() + "\n").getBytes(StandardCharsets.UTF_8));
        dataOutputStream.flush();
    }

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

    //send message to leader server
    public static void sendToLeader(JSONObject obj) throws IOException
    {
        Server destServer = CurrentServer.getInstance().getServers()
                .get( LeaderServices.getInstance().getLeaderID() );
        Socket socket = new Socket(destServer.getServerAddress(),
                destServer.getCoordinationPort());
        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        dataOutputStream.write((obj.toJSONString() + "\n").getBytes( StandardCharsets.UTF_8));
        dataOutputStream.flush();
    }
}
