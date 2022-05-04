import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class Week8Workshop {

    public static void main(String[] args) {     

        try{    
            
            Socket s=new Socket("localhost", 50000);

            DataOutputStream dout=new DataOutputStream(s.getOutputStream());
            BufferedReader dis=new BufferedReader(new InputStreamReader(s.getInputStream()));

            System.out.println("HELO reply = " + clientServerComm("HELO\n", dout, dis)); // client HELO

            System.out.println("AUTH reply = " + clientServerComm("AUTH justin\n", dout, dis)); // client AUTH

            String redyInfo = clientServerComm("REDY\n", dout, dis);
            System.out.println("REDY reply = " + redyInfo); // client REDY

            String serverMsg = redyInfo.substring(0,4); // client REDY

            while (!serverMsg.equals("NONE")){

                //filtering jobID from REDY reply
                String jobIdFilter[] = redyInfo.split(" ");
                int jobId = Integer.parseInt(jobIdFilter[2]);

                
                if (serverMsg.equals("JOBN")){
                    
                    int jobCore = Integer.parseInt(jobIdFilter[4]);
                    int jobMemory = Integer.parseInt(jobIdFilter[5]);
                    int jobDisk = Integer.parseInt(jobIdFilter[6]);
                    

                    // GETS Capable core memory disk

                    String getsInfo = clientServerComm("GETS Capable " + jobCore + " " + jobMemory + " " + jobDisk + "\n" , dout, dis);

                    System.out.println("GETS reply = " + getsInfo);
                    
                    //filtering server count from GETS reply
                    String serverCountFilter[] = getsInfo.split(" ");
                    int serverCount = Integer.parseInt(serverCountFilter[1]);
                    
                    dout.write(("OK\n").getBytes());
                    dout.flush();
                    System.out.println("OK reply = ");

                    List<String> serverList = new ArrayList<String>();

                    for (int i = 0; i < serverCount; i++){
                        serverList.add((String)dis.readLine());
                    }

                    System.out.println("OK reply = " + clientServerComm("OK\n", dout, dis)); // client OK

                    String serverType = serverList.get(0).split(" ")[0];
                    String serverId = serverList.get(0).split(" ")[1];
                
                    System.out.println("SCHD Job " + jobId + " response: " + clientServerComm("SCHD " + jobId + " " + serverType + " "+ serverId + "\n", dout, dis)); 
                }

                redyInfo = clientServerComm("REDY\n", dout, dis);
                System.out.println("REDY reply = " + redyInfo); // client REDY
                serverMsg = redyInfo.substring(0,4);                
            }

            
            System.out.println("QUIT reply = " + clientServerComm("QUIT\n", dout, dis)); // client QUIT
            
            
            dout.close();
            s.close();
        } catch(Exception e){System.out.println(e);}
    }

    public static String clientServerComm(String msgToServer, DataOutputStream dout, BufferedReader dis) throws IOException{
        
        dout.write(msgToServer.getBytes());
        dout.flush();

        String str=(String)dis.readLine();
        return str;
    }


   
}