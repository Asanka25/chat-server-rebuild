import handlers.ClientThreadHandler;
import consensus.FastBully;
import heartbeat.ConsensusJob;
import heartbeat.GossipJob;
import constants.Constant;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import handlers.ServerHandlerThread;
import models.CurrentServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Arrays;

public class Main {
    private static Integer alive_interval = 3;
    private static Integer alive_error_factor = 5;
    private static Boolean isGossip = true;
    private static Integer consensus_interval=10;
    private static Integer consensus_vote_duration=5;

    public static void main(String[] args) {

//        System.out.println("INFO : Enter server ID (ex- s1):  ");
//        Scanner scanner = new Scanner(System.in);
//        String serverID = scanner.nextLine();  // Read user input
//        System.out.println("LOG  : ARG[0] = " + args[0] + " ARG[1] = '" + args[1] + "'");
        CurrentServer.getInstance().initializeWithConfigs("s3", "/home/dilanka_rathnasiri/Documents/chat-server-rebuild/server_conf.txt");

        try {
            // throw exception if invalid server id provided
            if( CurrentServer.getInstance().getServerAddress() == null ) {
                throw new IllegalArgumentException();
            }

            //Coordination server socket
            ServerSocket coordinationServerSocket = new ServerSocket();
            SocketAddress endPointCoordination = new InetSocketAddress(
                    CurrentServer.getInstance().getServerAddress(),
                    CurrentServer.getInstance().getCoordinationPort()
            );
            coordinationServerSocket.bind( endPointCoordination );
            System.out.println( coordinationServerSocket.getLocalSocketAddress() );

            System.out.println("Coordination server socket address: "+ CurrentServer.getInstance().getServerAddress());
            System.out.println("Coordination server socket port: " + CurrentServer.getInstance().getCoordinationPort());

            ServerHandlerThread serverHandlerThread = new ServerHandlerThread(coordinationServerSocket);
            serverHandlerThread.start();

            /**
             Maintain consensus using Bully Algorithm
             **/
            FastBully.initialize();

            Runnable heartbeat = new FastBully("Heartbeat");
            new Thread(heartbeat).start();

            /**
             Heartbeat detection using gossiping
             **/
//            startGossip();
//            Runnable gossip = new GossipJob();
//            new Thread(gossip).start();
            if (isGossip) {
                System.out.println("INFO : Failure Detection is running GOSSIP mode");
                startGossip();
                startConsensus();
            }


            // Client connection server socket
            ServerSocket clientServerSocket = new ServerSocket();
            SocketAddress endPointClient = new InetSocketAddress(
                    CurrentServer.getInstance().getServerAddress(),
                    CurrentServer.getInstance().getClientsPort()
            );
            clientServerSocket.bind(endPointClient);

            System.out.println("Client connection server socket address: "+ CurrentServer.getInstance().getServerAddress());
            System.out.println("Client connection server socket port: " + CurrentServer.getInstance().getClientsPort());

            while (true) {
                ClientThreadHandler clientThreadHandler = new ClientThreadHandler(clientServerSocket.accept());
                CurrentServer.getInstance().addClientHandlerThreadToMap(clientThreadHandler);
                clientThreadHandler.start();
            }
        }
        catch( IllegalArgumentException | IOException | IndexOutOfBoundsException e ) {
            e.printStackTrace();
        }
    }

    private static void startGossip() {
        try {

            JobDetail gossipJob = JobBuilder.newJob(GossipJob.class)
                    .withIdentity(Constant.GOSSIP_JOB, "group1").build();

            gossipJob.getJobDataMap().put("aliveErrorFactor", alive_error_factor);

            Trigger gossipTrigger = TriggerBuilder
                    .newTrigger()
                    .withIdentity(Constant.GOSSIP_JOB_TRIGGER, "group1")
                    .withSchedule(
                            SimpleScheduleBuilder.simpleSchedule()
                                    .withIntervalInSeconds(alive_interval).repeatForever())
                    .build();

            Scheduler scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.start();
            scheduler.scheduleJob(gossipJob, gossipTrigger);

        } catch (SchedulerException e) {
            System.out.println("ERROR : Error in starting gossiping");
        }
    }

    private static void startConsensus() {
        try {

            JobDetail consensusJob = JobBuilder.newJob(ConsensusJob.class)
                    .withIdentity(Constant.CONSENSUS_JOB, "group1").build();

            consensusJob.getJobDataMap().put("consensusVoteDuration", consensus_vote_duration);

            Trigger consensusTrigger = TriggerBuilder
                    .newTrigger()
                    .withIdentity(Constant.CONSENSUS_JOB_TRIGGER, "group1")
                    .withSchedule(
                            SimpleScheduleBuilder.simpleSchedule()
                                    .withIntervalInSeconds(consensus_interval).repeatForever())
                    .build();

            Scheduler scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.start();
            scheduler.scheduleJob(consensusJob, consensusTrigger);

        } catch (SchedulerException e) {
            System.out.println("ERROR : Error in starting consensus");
        }
    }
}
