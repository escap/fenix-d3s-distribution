package org.fao.ess.cache.d3s;

public class SSHCommandTest {


    public static void main (String... args) throws Exception {
        /*
        Runtime rt = Runtime.getRuntime();
        Process pr = rt.exec("ssh exldvsdmxreg1.ext.fao.org rm /work/prod/nginx/cache/d3s/codes/89f051a750ac3c32ba9884bbb583df38");
        pr.waitFor();
        BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
        for (String line=in.readLine() ; line!=null ; line=in.readLine())
            System.out.println(line);
            */

        System.out.println("sendCommand");

        /**
         * YOU MUST CHANGE THE FOLLOWING
         * FILE_NAME: A FILE IN THE DIRECTORY
         * USER: LOGIN USER NAME
         * PASSWORD: PASSWORD FOR THAT USER
         * HOST: IP ADDRESS OF THE SSH SERVER
         **/
        String command = "ls /work/prod/nginx/cache/d3s/codes";
        String userName = "root";
        String password = "dasda7!jKJhJH65";
        String connectionIP = "exldvsdmxreg1.ext.fao.org";
        long time = System.currentTimeMillis();

        SSHManager instance = new SSHManager(userName, password, connectionIP, "");
        String errorMessage = instance.connect();

        if(errorMessage != null)
        {
            System.out.println(errorMessage);
        } else {
            String result = instance.sendCommand(command);
            time = System.currentTimeMillis()-time;

            instance.close();

            System.out.println(result + "\n\n"+time+" ms");
        }
    }
}
