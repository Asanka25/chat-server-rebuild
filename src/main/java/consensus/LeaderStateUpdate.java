package consensus;

import services.CoordinationServices;
import messaging.ServerMessage;
import models.Server;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import models.CurrentServer;
import services.LeaderServices;

import java.util.List;

public class LeaderStateUpdate extends Thread {

    int numberOfServersWithLowerIds = CurrentServer.getInstance().getSelfID() - 1;
    int numberOfUpdatesReceived = 0;
    volatile boolean leaderUpdateInProgress = true;

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        long end = start + 5000;
        try {

            while ( leaderUpdateInProgress ) {
                if( System.currentTimeMillis() > end || numberOfUpdatesReceived == numberOfServersWithLowerIds ) {
                    leaderUpdateInProgress = false;
                    System.out.println("INFO : Leader update completed");

                    FastBully.leaderUpdateComplete = true;

                    // add self clients and chat rooms to leader state
                    List<String> selfClients = CurrentServer.getInstance().getClientIdList();
                    List<List<String>> selfRooms = CurrentServer.getInstance().getChatRoomList();

                    for( String clientID : selfClients ) {
                        LeaderServices.getInstance().addClientLeaderUpdate(clientID);
                    }

                    for( List<String> chatRoom : selfRooms ) {
                        LeaderServices.getInstance().addApprovedRoom( chatRoom.get( 0 ),
                                chatRoom.get( 1 ), Integer.parseInt(chatRoom.get( 2 )) );
                    }
                    System.out.println("INFO : Finalized clients: " + LeaderServices.getInstance().getClientIDList() +
                            ", rooms: " + LeaderServices.getInstance().getRoomIDList());

                    // send update complete message to other servers
                    for ( int key : CurrentServer.getInstance().getServers().keySet() ) {
                        if ( key != CurrentServer.getInstance().getSelfID() ){
                            Server destServer = CurrentServer.getInstance().getServers().get(key);

                            try {
                                CoordinationServices.sendServer(
                                        ServerMessage.getLeaderStateUpdateComplete( String.valueOf(CurrentServer.getInstance().getSelfID()) ),
                                        destServer
                                );
                                System.out.println("INFO : Sent leader update complete message to s"+destServer.getServerID());
                            }
                            catch(Exception e) {
                                System.out.println("WARN : Server s"+destServer.getServerID()+
                                        " has failed, it will not receive the leader update complete message");
                            }
                        }
                    }
                }
                Thread.sleep(10);
            }

        } catch( Exception e ) {
            System.out.println( "WARN : Exception in leader update thread" );
        }

    }

    // update client list and chat room list of leader
    public void receiveUpdate( JSONObject j_object ) {
        numberOfUpdatesReceived += 1;
        JSONArray clientIdList = ( JSONArray ) j_object.get( "clients" );
        JSONArray chatRoomsList = ( JSONArray ) j_object.get( "chatrooms" );
        //System.out.println(chatRoomsList);

        for( Object clientID : clientIdList ) {
            LeaderServices.getInstance().addClientLeaderUpdate( clientID.toString() );
        }

        for( Object chatRoom : chatRoomsList ) {
            JSONObject j_room = (JSONObject)chatRoom;
            LeaderServices.getInstance().addApprovedRoom( j_room.get("clientid").toString(),
                    j_room.get("roomid").toString(), Integer.parseInt(j_room.get("serverid").toString()) );
        }
    }

}
