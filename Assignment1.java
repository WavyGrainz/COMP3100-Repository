import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class Assignment1 {
        
        private static int serverId = 0;
        private static int maxCores = 0;
        private static String maxServerType = null;
        private static String redyInfo = null;
        private static String serverMsg = null;
        private static List<String> servers = new ArrayList<String>();
        private static List<String> maxCoreServers = new ArrayList<String>();
        private static List<String> newMaxCoreServers = new ArrayList<String>();
        private static boolean onePass = true;
        private static boolean showServers = true;

        public static void main(String[] args) {

                try {

                        Socket dsSocket = new Socket("localhost", 50000);
                        DataOutputStream dataOut = new DataOutputStream(dsSocket.getOutputStream());
                        BufferedReader dataIn = new BufferedReader(new InputStreamReader(dsSocket.getInputStream()));

                        System.out.println("Connecting to ds-server...");
                        sendReply("HELO\n", dataOut);
                        System.out.println(readResponse(dataIn));

                        System.out.println("Authenticating justin...");
                        sendReply("AUTH justin\n", dataOut);
                        System.out.println(readResponse(dataIn));

                        sendReply("REDY\n", dataOut);
                        redyInfo = readResponse(dataIn);
                        serverMsg = redyInfo.substring(0, 4);
                        System.out.println("Readying system: " + redyInfo);

                        while (!serverMsg.equals("NONE")) {

                                String getJobIdString[] = redyInfo.split(" ");
                                int jobId = Integer.parseInt(getJobIdString[2]);
                                
                                sendReply("GETS All\n", dataOut);
                                String getsReply = readResponse(dataIn);

                                    while(showServers){
                                        System.out.println("Current server state information: " + getsReply);
                                        showServers = false;
                                        System.out.println("Finding highest core count:");
                                        }

                                String getServerCountString[] = getsReply.split(" ");
                                int serverCount = Integer.parseInt(getServerCountString[1]);

                                sendReply("OK\n", dataOut);

                                for (int i = 0; i < serverCount; i++) {
                                        servers.add((String)dataIn.readLine());
                                }

                                sendReply("OK\n", dataOut);
                                readResponse(dataIn);

                                for (int i = 0; i < servers.size(); i++) {
                                        String getMax[] = servers.get(i).split(" ");
                                        int coreCount = Integer.parseInt(getMax[4]);
                                        if (coreCount > maxCores) {
                                                maxCores = coreCount;
                                                maxServerType = getMax[0];
                                                System.out.println("Querying...'" + maxServerType
                                                                + " " + serverId + "' has " + maxCores + " cores available.");
                                        }
                                }

                                while (onePass) {
                                        
                                        for (int i = 0; i < servers.size(); i++) {
                                                int serverCores = Integer.parseInt(servers.get(i).split(" ")[4]);
                                                String serverType = servers.get(i).split(" ")[0];

                                                if (serverCores == maxCores && serverType.equals(maxServerType)) {
                                                        newMaxCoreServers.add(servers.get(i));
                                                }
                                        }

                                        onePass = false;
                                        maxCoreServers = newMaxCoreServers;
                                }

                                if (serverMsg.equals("JOBN")) {
                                        sendReply("SCHD " + jobId + " " + maxServerType + " " + serverId + "\n", dataOut);    
                                        System.out.println("Scheduling Job " + jobId + " on " + maxServerType + " " + serverId + "..." + readResponse(dataIn) + ".");
                                        serverId++;
                                }

                                if (serverId >= maxCoreServers.size()) {
                                        serverId = 0;
                                }

                                sendReply("REDY\n", dataOut);
                                redyInfo = readResponse(dataIn);
                                serverMsg = redyInfo.substring(0, 4);
                        }

                        System.out.println("All queued jobs scheduled. Exiting.");
                        sendReply("QUIT\n", dataOut);
                        readResponse(dataIn);

                        dataOut.close();

                        dsSocket.close();
                }

                catch (Exception e) {
                        System.out.println(e);
                }
        }

        public static void sendReply(String reply, DataOutputStream dataOut) throws IOException{
                dataOut.write(reply.getBytes());
                dataOut.flush(); 
        }

        public static String readResponse(BufferedReader dataIn) throws IOException{
                String response = (String)dataIn.readLine();
                return response;
        }
}
