/***************************************************************
  

   ____ ___  __  __ ____ _____ _  ___   ___                  
  / ___/ _ \|  \/  |  _ \___ // |/ _ \ / _ \                 
 | |  | | | | |\/| | |_) ||_ \| | | | | | | |                
 | |__| |_| | |  | |  __/___) | | |_| | |_| |                
  \____\___/|_|  |_|_|  |____/|_|\___/ \___/         _     _ 
    / \   ___ ___(_) __ _ _ __  _ __ ___   ___ _ __ | |_  / |
   / _ \ / __/ __| |/ _` | '_ \| '_ ` _ \ / _ \ '_ \| __| | |
  / ___ \\__ \__ \ | (_| | | | | | | | | |  __/ | | | |_  | |
 /_/   \_\___/___/_|\__, |_| |_|_| |_| |_|\___|_| |_|\__| |_|
                     |___/                                    

Author: Justin Khamis
Student ID: 45324328
**************************************************************/

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class Assignment1 {
        
        private static int serverId = 0;
        private static int mostCores = 0;
        private static String mostCoresServer = null;
        private static String redyResponse = null;
        private static String serverReply = null;
        private static List<String> allServersList = new ArrayList<String>();
        private static List<String> mostCoreList = new ArrayList<String>();
        private static List<String> mostCoreListCopy = new ArrayList<String>();
        private static boolean onePass = true;
        private static boolean showServers = true;

        public static void main(String[] args) {

                try {
                        /* Initialise dsSocket port 50000, dataOut and dataIn */
                        Socket dsSocket = new Socket("localhost", 50000);
                        DataOutputStream dataOut = new DataOutputStream(dsSocket.getOutputStream());
                        BufferedReader dataIn = new BufferedReader(new InputStreamReader(dsSocket.getInputStream()));

                        /* Begin ds-sim protocol by sending and reading responses for the following: 
                        HELO, AUTH, REDY. Print ds-server responses to console */
                        System.out.println("Sending HELO to ds-server...");
                        sendReply("HELO\n", dataOut);
                        System.out.println(readResponse(dataIn));

                        System.out.println("Authenticating justin...");
                        sendReply("AUTH justin\n", dataOut);
                        System.out.println(readResponse(dataIn));

                        sendReply("REDY\n", dataOut);
                        redyResponse = readResponse(dataIn);
                        serverReply = redyResponse.substring(0, 4);
                        System.out.println("Readying system: " + redyResponse);

                        while (!serverReply.equals("NONE")) {

                                /* Break jobId number out from redyInfo response and store in getJobIdString */
                                String getJobIdString[] = redyResponse.split(" ");
                                int jobId = Integer.parseInt(getJobIdString[2]);
                                
                                /* Retrieve server state information with GETS All command, save response to
                                getsReply. Print GETS All information to console only once while showServers 
                                boolean is active */
                                sendReply("GETS All\n", dataOut);
                                String getsReply = readResponse(dataIn);

                                    while(showServers){
                                        System.out.println("Current server state information: " + getsReply);
                                        showServers = false;
                                        System.out.println("Finding highest core count:");
                                        }

                                
                                /* Break serverCount out from getsReply response and store in getServerCountString. 
                                serverCount integer saves number of servers by using parseInt */
                                String getServerCountString[] = getsReply.split(" ");
                                int serverCount = Integer.parseInt(getServerCountString[1]);

                                /* Send OK to server */
                                sendReply("OK\n", dataOut);

                                /* Loop to add server information to allServersList from the data input stream
                                of GETS All command */
                                for (int i = 0; i < serverCount; i++) {
                                        allServersList.add((String)dataIn.readLine());
                                }

                                /* Send OK to server */
                                sendReply("OK\n", dataOut);
                                readResponse(dataIn);

                                /* Loop to determine core count of servers in allServersList; 
                               Each object is stored in MostCoresArray and then number of cores in coreCount.
                               Loop to find the highest amount of cores is then run and then servers matching
                               the mostCores are saved in mostCoresServer*/
                                for (int i = 0; i < allServersList.size(); i++) {
                                        String mostCoresArray[] = allServersList.get(i).split(" ");
                                        int coreCount = Integer.parseInt(mostCoresArray[4]);
                                        if (coreCount > mostCores) {
                                                mostCores = coreCount;
                                                mostCoresServer = mostCoresArray[0];
                                                System.out.println("Querying...'" + mostCoresServer
                                                                + " " + serverId + "' has " + mostCores + " cores available.");
                                        }
                                }

                                /* Boolean condtion to copy maxCoreServers after first loop.
                                GETS All is causing servers to move into a busy state after jobs have been
                                scheduled, causing mostCoreList reduce in size. */
                                while (onePass) {
                                        
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

                                /* Sending commands to SCHD jobs on mostCoresServer when serverReply equals JOBN. Loop iterates 
                                through serverId integer. ServerId is reset to 0 once mostCoreList has finished iterating and
                                client sends REDY command to ds-server for next schedule to commence */
                                if (serverReply.equals("JOBN")) {
                                        sendReply("SCHD " + jobId + " " + mostCoresServer + " " + serverId + "\n", dataOut);    
                                        System.out.println("Scheduling Job " + jobId + " on " + mostCoresServer + " " + serverId + 
                                        "..." + readResponse(dataIn) + ".");
                                        serverId++;
                                }

                                if (serverId >= mostCoreList.size()) {
                                        serverId = 0;
                                }

                                sendReply("REDY\n", dataOut);
                                redyResponse = readResponse(dataIn);
                                serverReply = redyResponse.substring(0, 4);
                        }

                        /* Once serverReply equals NULL, QUIT is sent to server and stream + socket are closed */
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

        /* Method sendReply to write dataOut and flush stream */
        public static void sendReply(String reply, DataOutputStream dataOut) throws IOException{
                dataOut.write(reply.getBytes());
                dataOut.flush(); 
        }

        /* Method readResponse to read dataIn stream and return response
        to main method */
        public static String readResponse(BufferedReader dataIn) throws IOException{
                String response = (String)dataIn.readLine();
                return response;
        }
}
