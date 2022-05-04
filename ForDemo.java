import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class ForDemo {

        private static int serverId = 0;
        private static int mostCores = 0;
        private static String mostCoresServer = null;
        private static List<String> mostCoreList = new ArrayList<String>();

        public static void main(String[] args) {

                try {

                        Socket dsSocket = new Socket("localhost", 50000);
                        DataOutputStream dataOut = new DataOutputStream(dsSocket.getOutputStream());
                        BufferedReader dataIn = new BufferedReader(new InputStreamReader(dsSocket.getInputStream()));

                        String redyResponse = null;
                        String serverReply = null;

                        System.out.println("Connecting to ds-server...");
                        sendReply("HELO\n", dataOut);
                        System.out.println((String)dataIn.readLine());

                        System.out.println("Authenticating justin...");
                        sendReply("AUTH justin\n", dataOut);
                        System.out.println((String)dataIn.readLine());

                        sendReply("REDY\n", dataOut);
                        redyResponse = (String)dataIn.readLine();
                        serverReply = redyResponse.substring(0, 4);
                        System.out.println("Readying system: " + redyResponse);

                        boolean onePass = true;

                        while (!serverReply.equals("NONE")) {

                                String getJobIdString[] = redyResponse.split(" ");
                                int jobId = Integer.parseInt(getJobIdString[2]);

                                sendReply("GETS All\n", dataOut);
                                String getsReply = (String)dataIn.readLine();

                               /* while (showServers) {
                                        System.out.println("Current server state information: " + getsReply);
                                        showServers = false;
                                        System.out.println("Finding highest core count:");
                                } */

                                String getServerCountString[] = getsReply.split(" ");
                                int serverCount = Integer.parseInt(getServerCountString[1]);

                                sendReply("OK\n", dataOut);
                                List<String> allServersList = new ArrayList<String>();

                                for (int i = 0; i < serverCount; i++) {
                                        allServersList.add((String) dataIn.readLine());
                                }

                                sendReply("OK\n", dataOut);
                                dataIn.readLine();

                                for (int i = 0; i < allServersList.size(); i++) {
                                        String mostCoresArray[] = allServersList.get(i).split(" ");
                                        int coreCount = Integer.parseInt(mostCoresArray[4]);
                                        if (coreCount > mostCores) {
                                                mostCores = coreCount;
                                                mostCoresServer = mostCoresArray[0];
                                                System.out.println("Querying...'" + mostCoresServer
                                                                + " " + serverId + "' has " + mostCores
                                                                + " cores available.");
                                        }
                                }
                                

                                while (onePass) {
                                    List<String> mostCoreListCopy = new ArrayList<String>();
                                        for (int i = 0; i < allServersList.size(); i++) {
                                                int serverCores = Integer.parseInt(allServersList.get(i).split(" ")[4]);
                                                String serverType = allServersList.get(i).split(" ")[0];

                                                if (serverCores == mostCores && serverType.equals(mostCoresServer)) {
                                                        mostCoreListCopy.add(allServersList.get(i));
                                                }
                                        }

                                        onePass = false;
                                        mostCoreList = mostCoreListCopy;
                                }

                                if (serverReply.equals("JOBN")) {
                                        sendReply("SCHD " + jobId + " " + mostCoresServer + " " + serverId + "\n",
                                                        dataOut);
                                        /*System.out.println("Scheduling Job " + jobId + " on " + mostCoresServer + " "
                                                        + serverId + "..." + (String)dataIn.readLine() + ".");*/
                                        dataIn.readLine();
                                        serverId++;
                                }

                                if (serverId >= mostCoreList.size()) {
                                        serverId = 0;
                                }

                                sendReply("REDY\n", dataOut);
                                redyResponse = (String)dataIn.readLine();
                                /*System.out.println(redyResponse);*/
                                serverReply = redyResponse.substring(0, 4);
                        }

                        System.out.println("All queued jobs scheduled. Exiting.");
                        sendReply("QUIT\n", dataOut);
                        System.out.println(dataIn.readLine());
                        dataOut.close();

                        dsSocket.close();
                }

                catch (Exception e) {
                        System.out.println(e);
                }
        }

        public static void sendReply(String reply, DataOutputStream dataOut) throws IOException {
                dataOut.write(reply.getBytes());
                dataOut.flush();
        }

        /*public static String readResponse(BufferedReader dataIn) throws IOException {
                String response = (String) dataIn.readLine();
                return response;
        }*/
}
