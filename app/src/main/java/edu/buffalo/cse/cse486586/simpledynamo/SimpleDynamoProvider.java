package edu.buffalo.cse.cse486586.simpledynamo;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Formatter;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Comparator;
import android.database.MergeCursor;
import android.content.Context;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class SimpleDynamoProvider extends ContentProvider {

    /*
   */
    int port_number=0;
    private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    public static TreeMap<String, String> chord_ring;
    private HashMap<String,String> single_query = new HashMap();
    private HashMap<String,String> star_queries = new HashMap();
    private static final String REMOTE_PORTS[] = {"11108","11112","11116","11120","11124"};
    private static final String AVDS[] = {"5554","5556","5558","5560","5562"};
    private static final int SERVER_PORT = 10000;
    private static final String STAR_SYMBOL = "*";
    private static final String AT_SYMBOL = "@";
    private static final String NOTHING = "xxxxx";
    private static final String NEWJOIN = "NEWJOIN";
    private static final String DELETE = "DELETE";
    private static final String DELETE_ALL = "DELETE_ALL";
    private static final String DELETE_ALL_REPLY = "DELETE_ALL_REPLY";
    private static final String GETMINE = "GETMINE";
    private static final String INSERT = "INSERT";
    private static final String QUERY = "QUERY";
    private static final String QUERY_ALL = "QUERY_ALL";
    private static final String QUERY_REPLY = "QUERY_REPLY";
    private static final String QUERY_ALL_REPLY = "QUERY_ALL_REPLY";
    private static final String GETMINE_REPLY = "GETMINE_REPLY";
    private static final String UPDATE = "UPDATE";
    private static final String REPLICATE = "REPLICATE";

    private static final String INSERTED = "INSERTED";
    private static String head;
    private static String pred_hash_id;
    private static String succ_hash_id;
    private static String pred_port_id;
    private static String succ_port_id;
    private static String my_node_id;
    private static String my_port_id;
    private static final String myDelimiter = "###";
    private static final String delim = "!!!!!!";
    private static String headpred;
    private static String headsucc;
    private final Uri myUri = makeUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    private Boolean foundOrigin = false;
    private int responseNum = 0;
    private int getThreeResponse = 0;
    private int delResponse = 0;
    private Boolean deleteSingle = false;
    private Boolean deleteAll = false;
    private static String replica_port1;
    private static String replica_port2;
    private static String replica_hash1;
    private static String replica_hash2;
    private static String pred_port_id2;
    private static String pred_hash_id2;
    private int delOnCreate = 0;
    /*
     */
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
  /* My code starts here*/
        String qType = selection;
        String key_val[] = {"key", "value"};
        String sb = "";
        Log.v("IN DELETE...........", "");
        Log.v("Delete ","selection is "+ selection);
        // same as query almost
        if(qType.compareTo(STAR_SYMBOL)==0)
        {
            Log.v("Delete ","STAR Delete "+ selection);
            int c = 0;
            try {
                BufferedReader reader = null;

                File file_path = new File(getContext().getFilesDir().getAbsolutePath());
                if (file_path.listFiles() != null) {
                    for (File f : file_path.listFiles()) {
                        if(f.delete())
                        {
                            c ++;
                        }
                    }
                    reader.close();
                }
            } catch (Exception e) {
                Log.e("DELETE EXCEPTION", " *, file reading has failed");
            }
            // Send a message to the next one requesting for all messages to be deleted from their AVDs.
            String msgToSucc = "";
            delResponse = 0;
            msgToSucc = DELETE_ALL+myDelimiter+qType+myDelimiter+my_port_id;
            for(int i=0;i<5;i++)
            {
                if(REMOTE_PORTS[i].compareTo(my_port_id)!=0)
                {
                    sendMsg(msgToSucc, REMOTE_PORTS[i]);
                }
            }
            while(delResponse!=4)
            {}
            return c;
        }
        else if(qType.compareTo(AT_SYMBOL)==0)
        {
            Log.v("Delete ","AT Delete "+ selection);
            // just get all from here
            try {
                BufferedReader reader = null;
                int c = 0;
                File file_path = new File(getContext().getFilesDir().getAbsolutePath());
                if (file_path.listFiles() != null) {
                    for (File f : file_path.listFiles()) {
                        if(f.delete())
                        {
                            c ++;
                        }
                    }

                    reader.close();
                }
                return c;
            } catch (Exception e) {
                Log.e("DELETE EXCEPTION", " @,file reading has failed");
            }
        }
        else
        {
            Log.v("Delete ","Delete, key is: "+ selection);
            File file = new File(getContext().getFilesDir(),qType);
            if(file.exists()){
                //if current avd has file
                try {
                    if(file.delete())
                    {
                        Log.v("Delete has file"," Deleted "+ selection);
                        // only one file to be deleted
                        return 1;
                    }
                    String msgToSucc = "";
                    msgToSucc = DELETE+myDelimiter+qType+myDelimiter+my_port_id;
                    String rep1 = getMySuccessor(genHash(my_port_id));
                    String rep2 = getMyReplica2(genHash(my_port_id));
                    Log.v("Delete has file","sending messages to replicas about"+ selection);
                    Log.v("Del to replica1 ",", "+ getPortNumber(rep1));
                    Log.v("Del to replica2 ",", "+ getPortNumber(rep2));
                    sendMsg(msgToSucc,getPortNumber(rep1));
                    sendMsg(msgToSucc,getPortNumber(rep2));

                }catch(NoSuchAlgorithmException e) {
                }
                catch(NullPointerException e) {
                    Log.e("DELETE EXCEPTION ","NPE");
                }
            }
            else{
                // else send message to succ to delete required data
                String msgToSucc = "";
                msgToSucc = DELETE+myDelimiter+qType+myDelimiter+my_port_id;
                String key_hash1="";
                try {
                    key_hash1 = genHash(qType);
                }
                catch(NoSuchAlgorithmException e) {
                }
                String portSendID = getPortToInsert(key_hash1);
                String rep1="";
                String rep2="";
                int psID = Integer.parseInt(portSendID)/2;
                try{
                    rep1 = getMySuccessor(genHash(Integer.toString(psID)));
                    rep2 = getMyReplica2(genHash(Integer.toString(psID)));
                }
                catch(NoSuchAlgorithmException e){}
                Log.v("Delete has no file ","sending messages to 3 units about "+ selection);
                Log.v("Del to main unit ",", "+ portSendID);
                Log.v("Del to replica1 ",", "+ getPortNumber(rep1));
                Log.v("Del to replica2 ",", "+ getPortNumber(rep2));
                sendMsg(msgToSucc, portSendID);
                sendMsg(msgToSucc,getPortNumber(rep1));
                sendMsg(msgToSucc,getPortNumber(rep2));

//                while(deleteSingle!=true)
//                {}
//                deleteSingle = false;
            }
        }
        Log.v("Delete end ","selection: "+selection);

  /* My code ends here*/
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try{
            Log.v("IN INSERT..............", "");
            String key_data = values.get("key").toString();
            Log.v("key is: ", key_data);
            String value_data = values.get("value").toString();
            Log.v("value is: ", value_data);
            String key_hash = genHash(key_data);
            String portToInsert = getPortToInsert(key_hash);
            int portToIns = Integer.parseInt(portToInsert)/2;
            String portToInsertHash =genHash(Integer.toString(portToIns));
            Log.v("Insert", " portToInsert is :  " + portToInsert);
            Log.v("Insert", "Key Hash : "+key_hash+ "PortInsert Hash :  " + portToInsertHash);
            String hashToRep1 =	getMySuccessor(portToInsertHash);
            String hashToRep2 = getMyReplica2(portToInsertHash);
            String portToRep1 = getPortNumber(hashToRep1);
            String portToRep2 = getPortNumber(hashToRep2);
            //
            //sending messages to Port, Rep1, Rep2.
            String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
            value_data = value_data+delim+timeStamp;
            String message = INSERT+myDelimiter+key_data+myDelimiter+value_data;
            String re_ports[] = {portToInsert, portToRep1, portToRep2};
            for(int k=0;k<3;k++)
            {
                try{
                    Socket socket111 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(re_ports[k]));
                    PrintWriter pw111 = new PrintWriter(socket111.getOutputStream(), true);
                    pw111.println(message);
                    Log.v("Insert", "Sending MSG "+message+" to : " + re_ports[k]);
                    BufferedReader in1 = new BufferedReader(new InputStreamReader(socket111.getInputStream()));
                    String inputss = in1.readLine();
                    String[] messages = inputss.split(myDelimiter);
                    Log.v("Insert", "Insert success at : " + re_ports[k]);
                }
                catch(Exception e)
                {
                    Log.e("INSERT Exception "," "+e);
                }
                ////  catch(NoSuchAlgorithmException e){}
            }
        }
        catch(NoSuchAlgorithmException e){

        }
        //Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        if(this.getContext()!=null)
        {

            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            my_port_id = myPort;
            port_number = Integer.parseInt(myPort);
            String hash_id="";
            Log.v("IN ONCREATE()........", "");
            try{
                //Log.v("in onCreate()", " Before server connection");
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
                Log.v("onCreate()", " After server connection");

            }catch(IOException e)
            {
                Log.e("ONCREATE EXCEPTION"," Can't create server sock");
            }
            try{
                hash_id = genHash(portStr);
            }catch(NoSuchAlgorithmException e)
            {
                Log.e("ONCREATE EXCEPTION", "No Such Algo ");
            }
            my_node_id = hash_id;
            Log.v("onCreate()", "My port is "+ myPort+ " and Hash is"+ hash_id);
            // tree map because it orders content
            chord_ring = new TreeMap<String, String>(
                    new Comparator<String>(){
                        @Override
                        public int compare(String ele1, String ele2)
                        {
                            return ele1.compareTo(ele2);
                        }
                    }
            );
            for(int i=4;i>=0;i--) {
                String hid ="";
                try{
                    hid = genHash(AVDS[i]);
                }catch(NoSuchAlgorithmException e)
                {
                    Log.e("ONCREATE EXCEPTION", "No Such Algo ");
                }
                chord_ring.put(hid, REMOTE_PORTS[i]);
            }

            replica_hash1 = getMySuccessor(my_node_id);
            replica_hash2 =	getMyReplica2(my_node_id);
            replica_port1 = getPortNumber(replica_hash1);
            replica_port2 = getPortNumber(replica_hash2);
            pred_hash_id = getMyPredecessor(my_node_id);
            succ_hash_id = getMySuccessor(my_node_id);
            pred_port_id = getPortNumber(pred_hash_id);
            succ_port_id = getPortNumber(succ_hash_id);
            pred_hash_id2 = getMyPredecessor2(my_node_id);
            pred_port_id2 = getPortNumber(pred_hash_id2);
            //delete everything I have
            try {
                BufferedReader reader = null;
                int c = 0;
                File file_path = new File(getContext().getFilesDir().getAbsolutePath());
                if (file_path.listFiles() != null) {
                    for (File f : file_path.listFiles()) {
                        if(f.delete())
                        {
                            c ++;
                        }
                    }

                    reader.close();
                }
                //delOnCreate = 1;
                //return c;
            } catch (Exception e) {
                delOnCreate = 0;
                Log.e("MY DELETEALL EXCEPTION", "filevcxz reading has failed");
            }
            Log.v("in onCreate()", "My port id is: "+my_port_id);
            Log.v("in onCreate()", "My p1 id is: "+pred_port_id);
            Log.v("in onCreate()", "My p2 id is: "+pred_port_id2);
            Log.v("in onCreate()", "My s1 id is: "+succ_port_id);
            Log.v("in onCreate()", "My s2 id is: "+replica_port2);
            Log.v("in onCreate()", "My r1 id is: "+replica_port1);
            Log.v("in onCreate()", "My r2 id is: "+replica_port2);
            Log.v("in onCreate()", "Re_Join Initiating...........");
            getThreeResponse = 0;
            Log.v("onCreate()", " Re_Join responses before : " + getThreeResponse);
            new Re_Join().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
            Log.v("onCreate()", " Re_Join responses after : " + getThreeResponse);
            //send messages to succ and pred1, pred2 to get messages
//            String messagex = GETMINE+myDelimiter+my_port_id;

//            sendMsg(messagex,pred_port_id);
//            sendMsg(messagex,pred_port_id2);
//            sendMsg(messagex,replica_port1);
//            //sendMsg(messagex,replica_port2);
//            Log.v("in onCreate()", "SentMessage to GEME, "+getThreeResponse);
//            //store the messages I got
            Log.v("ONCREATE()", " End of onCreate()");
//            while(getThreeResponse!=3)
//            {
//
//            }
//            try {
//
//                Log.v("in onCreate()", "Sleeping, "+getThreeResponse);
//                //Thread.sleep(2000);
//                Thread.sleep(350);
//
//
//            } catch (Exception e) {
//
//            }

            //Log.v("Insert", " portToInsert is :  " + portToInsert);
//            while((getThreeResponse!=3) || (getThreeResponse!=0))
//            {
//
//            }
            //Log.v("in onCreate()", "Putting into ring, port "+ port_number);
        }

        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
  /* My code starts here*/
        String qType = selection;
        String key_val[] = {"key", "value"};
        String sb = "";
        Log.v("IN QUERY...............", "");
        Log.v("Query ","selection is "+ selection);

        if(qType.compareTo(STAR_SYMBOL)==0)
        {
            // get from here and then send to successor for information
            Log.v("In Query "," * selection is "+ selection);
            MatrixCursor mc1 = new MatrixCursor(key_val);
            try {
                BufferedReader reader = null;
                String s = "";
                File file_path = new File(getContext().getFilesDir().getAbsolutePath());
                if (file_path.listFiles() != null) {
                    for (File f : file_path.listFiles()) {
                        s = f.getName();
                        reader = new BufferedReader(new FileReader(file_path + "/" + s));
                        String value1 = reader.readLine();
                        String[] valss = value1.split(delim);
                        value1 = valss[0];
                        String[] row2 = {
                                s, value1
                        };
                        mc1.addRow(row2);
                    }
                    reader.close();
                }
            } catch (Exception e) {
                Log.e("QUERY EXCEPTION", "file reading has failed");
            }
            // Send a message to the next one requesting to get all messages from their AVDs.
            responseNum = 0;
            String msgToSucc = "";
            msgToSucc = QUERY_ALL+myDelimiter+qType+myDelimiter+my_port_id;
            for(int i=0;i<5;i++)
            {
                if(REMOTE_PORTS[i].compareTo(my_port_id)!=0)
                {
                    sendMsg(msgToSucc, REMOTE_PORTS[i]);
                }
            }

            while(responseNum != 3)
            {
                //wait for all the replies from all avds
                // Log.v("QUERY_ALL_REPLY", "key : "+responseNum);
            }
            try {
                //Thread.sleep(1000);
                Thread.sleep(300);

            } catch (Exception e) {

            }
            MatrixCursor mc = new MatrixCursor(key_val);
            for(String key: star_queries.keySet())
            {
                String val = star_queries.get(key);
                String[] row1 = {
                        key, val
                };
                mc.addRow(row1);
            }
            // ref :https://developer.android.com/reference/android/database/MergeCursor.html
            MergeCursor finalCursor = new MergeCursor(new Cursor[]{mc1,mc});
            return finalCursor;

        }
        else if(qType.compareTo(AT_SYMBOL)==0)
        {
            Log.v("In Query "," @ selection is "+ selection);
            // just get all from here
            try {
                BufferedReader reader = null;
                String s = "";
                MatrixCursor mc = new MatrixCursor(key_val);
                File file_path = new File(getContext().getFilesDir().getAbsolutePath());
                if (file_path.listFiles() != null) {
                    for (File f : file_path.listFiles()) {
                        s = f.getName();
                        reader = new BufferedReader(new FileReader(file_path + "/" + s));
                        String value = reader.readLine();
                        String[] valss = value.split(delim);
                        String value1 = valss[0];
                        Log.v("In Query "," @ value is "+ value);
                        String[] row1 = {
                                s, value1
                        };
                        mc.addRow(row1);
                    }

                    reader.close();
                }
                return mc;
            } catch (Exception e) {
                Log.e("QUERY EXCEPTION", "file reading has failed");
            }
        }
        else
        {
            Log.v("In Query ","key selection is "+ selection);
            File file = new File(getContext().getFilesDir(),qType);
            MatrixCursor mc = new MatrixCursor(key_val);
            if(file.exists()){
                //if current avd has file
                Log.v("Query ","Has file "+ selection);
                try {
                    int size;
                    sb="";
                    FileInputStream in_stream = getContext().openFileInput(selection);
                    while ((size = in_stream.read()) != -1) {
                        sb += (char) size;
                        //sb+= Character.toString((char) size);
                    }
                    //Log.v("inside query 1","sb"+sb);
                    in_stream.close();
                    String[] valss = sb.split(delim);
                    sb = valss[0];
                    String row[] = new String[]{selection,sb};
                    mc.addRow(row);

                } catch (FileNotFoundException e) {
                    Log.e("QUERY EXCEPTION ","FNE");
                } catch (IOException e) {
                    Log.e("QUERY EXCEPTION ","IOE");
                } catch (NullPointerException e) {
                    Log.e("QUERY EXCEPTION ","NPE");
                }
            }
            else{
                // else send message to succ to get required data
                Log.v("Query ","Has no file "+ selection);
                String msgToSucc = "";
                msgToSucc = QUERY+myDelimiter+qType+myDelimiter+my_port_id;
                String key_hash1="";
                try {
                    key_hash1 = genHash(qType);
                }
                catch(NoSuchAlgorithmException e) {
                }
                String portSendID = getPortToInsert(key_hash1);
                Log.v("Query "," Asking "+qType+" To "+portSendID);
                //  sendMsg(msgToSucc, portSendID);
                int psID = Integer.parseInt(portSendID)/2;
                String sucHash="";
                try{
                    sucHash = getMySuccessor(genHash(Integer.toString(psID)));
                }
                catch(NoSuchAlgorithmException e){}
                Log.v("Query "," Asking "+qType+" To "+getPortNumber(sucHash));
                //  sendMsg(msgToSucc,getPortNumber(sucHash));

                String re_ports11[] = {portSendID, getPortNumber(sucHash)};
                String[] v = new String[2];
                String[] t = new String[2];
                for(int i= 0;i<2;i++)
                {
                    try{
                        Socket socket1111 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(re_ports11[i]));
                        PrintWriter pw1111 = new PrintWriter(socket1111.getOutputStream(), true);
                        pw1111.println(msgToSucc);
                        Log.v("Insert", "Sending MSG "+msgToSucc+" to : " + re_ports11[i]);
                        BufferedReader in11 = new BufferedReader(new InputStreamReader(socket1111.getInputStream()));
                        String inputss = in11.readLine();
                        String[] messages = inputss.split(myDelimiter);
                        String values = messages[2];
                        String[] messages2 = values.split(delim);
                        v[i] = messages2[0];
                        t[i] = messages2[1];
                        Log.v("Insert", "Insert success at : " + re_ports11[i]);
                    }
                    catch(Exception e)
                    {
                        Log.e("INSERT Exception "," "+e);
                    }
                }
                if(v[0]==null)
                {
                    single_query.put(qType, v[1]);
                }
                else if(v[1]==null)
                {
                    single_query.put(qType, v[0]);
                }
                else if((v[0]!=null) && (v[1]!=null) && (t[0]!=null) && (t[1]!=null))
                {
                    if (t[0].compareTo(t[1]) > 0)
                    {
                        single_query.put(qType, v[0]);
                    }
                    else
                    {
                        single_query.put(qType, v[1]);
                    }
                }
                while((single_query.get(selection) == null))
                {
                    //if current avd didn't receive the query from successors
                }
                Log.v("Query "," Got reply for "+qType+" as"+ single_query.get(selection));
                String[] row3 = {
                        selection, single_query.get(selection)
                };
                mc.addRow(row3);

            }
            return mc;
        }
        Log.v("Query end here ","selection: "+selection);
        return null;
    /* My code ends here*/
    }



    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private int getNodePosition(String key)
    {
        //Log.v("x", "in getNodePosition()");
        int pos = 0;
        for (String cr_key : chord_ring.keySet()) {

            if(cr_key.compareTo(key) == 0)
            {
                break;
            }
            pos++;
        }
        return pos;
    }

    private void deleteAll()
    {
        Log.v("MY DELETEALL ","..................................");
        try {
            BufferedReader reader = null;
            int c = 0;
            File file_path = new File(getContext().getFilesDir().getAbsolutePath());
            if (file_path.listFiles() != null) {
                for (File f : file_path.listFiles()) {
                    if(f.delete())
                    {
                        c ++;
                    }
                }

                reader.close();
            }
            delOnCreate = 1;
            //return c;
        } catch (Exception e) {
            delOnCreate = 0;
            Log.e("MY DELETEALL EXCEPTION", "filevcxz reading has failed");
        }
    }

    private String getPortToInsert(String key_hash)
    {
        int pos1 = 0;
        String hashval = (String) chord_ring.keySet().toArray()[pos1];
        String portval = chord_ring.get(hashval);
        for(String key : chord_ring.keySet())
        {
            if(key_hash.compareTo(key) <= 0)
            {
                hashval = (String) chord_ring.keySet().toArray()[pos1];
                portval = chord_ring.get(hashval);
                return portval;
            }
            pos1++;
        }
        return portval;
    }

    private Uri makeUri(String s, String a) {
        Uri.Builder ub = new Uri.Builder();
        ub.authority(a);
        ub.scheme(s);
        return ub.build();
    }

    private String getMySuccessor(String nodeValue)
    {
        //Log.v("x", "in getMySuccessor() " + nodeValue);
        String pred;
        int size = chord_ring.size();
        int position = getNodePosition(nodeValue);
        //Log.v("x", "in pos, size : " + position+","+size);
        String succ;
        if((position == (size-1)))
        {
            succ = (String) chord_ring.keySet().toArray()[0];
        }
        else{
            succ = (String) chord_ring.keySet().toArray()[position+1];
        }
        return succ;
    }

    private String getMyPredecessor(String nodeValue)
    {
        //Log.v("x", "in getMyPredecessor()");
        String pred;
        int size = chord_ring.size();
        int position = getNodePosition(nodeValue);
        if(position == 0)
        {
            pred = (String) chord_ring.keySet().toArray()[size-1];
        }
        else{
            pred = (String) chord_ring.keySet().toArray()[position-1];
        }
        return pred;
    }
    private String getMyPredecessor2(String nodeValue)
    {
        //Log.v("x", "in getMyPredecessor()");
        String pred;
        int size = chord_ring.size();
        int position = getNodePosition(nodeValue);
        if(position == 1)
        {
            pred = (String) chord_ring.keySet().toArray()[size-1];
        }
        else if(position == 0)
        {
            pred = (String) chord_ring.keySet().toArray()[size-2];
        }
        else{
            pred = (String) chord_ring.keySet().toArray()[position-2];
        }
        return pred;
    }

    private String getMyReplica2(String nodeValue)
    {
        //Log.v("x", "in getSuccessor() " + nodeValue);
        String pred;
        int size = chord_ring.size();
        int position = getNodePosition(nodeValue);
        String succ;
        if((position == (size-2)))
        {
            succ = (String) chord_ring.keySet().toArray()[0];
        }
        else if((position == (size-1)))
        {
            succ = (String) chord_ring.keySet().toArray()[1];
        }
        else{
            succ = (String) chord_ring.keySet().toArray()[position+2];
        }
        return succ;
    }



    private String getPortNumber(String hashValue)
    {
        //Log.v("x", "in getPortNumber()");
        String portval = chord_ring.get(hashValue);
        return portval;
    }

    private void sendMsg(String message, String remotePort)
    {
        Log.v("SendMSG()",".............................");
        try {
            Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(remotePort));
            PrintWriter pw1 = new PrintWriter(socket1.getOutputStream(), true);
            pw1.println(message);
            Log.v("SendMSG()","Sent "+" msg: "+message+" to port: "+remotePort);
//            socket1.close();
        }

//          catch (UnknownHostException e) {
//            Log.d("ClTask", "UnknownHostException");
//        } catch (IOException e) {
//            Log.d("ClTask", "socketIOException");
//        }
        catch (Exception e) {
            responseNum++;
            delResponse++;
            getThreeResponse++;
            Log.e("SendMSG() EXCEPTION", ""+e);
        }
    }

    private void printTreeMap()
    {
        if(my_port_id.compareTo("11108")==0) {
            Log.v("PrintTreeMap()", "Printing Tree Ring Values");
            for (Map.Entry<String, String> entry : chord_ring.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                System.out.printf("%s ::: %s\n", key, value);
            }
        }
    }
    private class Re_Join extends AsyncTask<Void, Void, Void> {
        @Override
        protected Void doInBackground(Void... voids) {
            // try {
            String messagex = GETMINE+myDelimiter+my_port_id;
            Log.v("REJOIN()","................................................");
            //String re_ports[] = {pred_port_id, pred_port_id2, replica_port1};
            //String re_ports[] = {pred_port_id, pred_port_id2, replica_port1};
            for(int k=0;k<5;k++)
            {
                try{
                    if(REMOTE_PORTS[k].compareTo(my_port_id)!=0){
                        Socket socket111 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORTS[k]));
                        PrintWriter pw111 = new PrintWriter(socket111.getOutputStream(), true);
                        pw111.println(messagex);
                        Log.v("RE_JOIN()","Request"+" msg: "+messagex+" to port: "+REMOTE_PORTS[k]);
                        BufferedReader in1 = new BufferedReader(new InputStreamReader(socket111.getInputStream()));
                        String inputss = in1.readLine();
                        String[] messages = inputss.split(myDelimiter);
                        Log.v("RE_JOIN()","Reply"+" msg: "+inputss+" from port: "+REMOTE_PORTS[k]);
                        int lens = messages.length;
                        if (lens > 1) {
                            int i = 1;
                            while (i < lens) {
                                String key_1 = messages[i];
                                String value_1 = messages[i + 1];
                                String key1_hash = genHash(key_1);
                                String port_vals = getPortToInsert(key1_hash);
                                i = i + 2;
                                if ((port_vals.compareTo(pred_port_id) == 0) || (port_vals.compareTo(my_port_id) == 0)
                                        || (port_vals.compareTo(pred_port_id2) == 0)) {
                                    //insert into current avd
                                    FileOutputStream out_stream_1 = getContext().openFileOutput(key_1, Context.MODE_PRIVATE);
                                    out_stream_1.write(value_1.getBytes());
                                    out_stream_1.close();
                                }

                            }
                        }
                        getThreeResponse++;
                    }
                }
                catch(Exception e)
                {
                    getThreeResponse++;
                    Log.e("Re_Task","Exception + "+e);
                    Log.e("Re_Task1","Exception + "+e.getStackTrace()[0].getLineNumber());
                }
                ////  catch(NoSuchAlgorithmException e){}
            }
            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            Log.v("CLIENTTASK()","...................................................");
            String message = msgs[0];
            String remotePort = msgs[1];
            //Log.v("CT", "msg "+message+" ,remoteport "+ remotePort);
            //sendMsg(message,remotePort);
            try {
                //Log.v("CT", "before creating socket");
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                //Log.v("CT", "Sending join message");
                PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                pw.println(message);
                //Log.v("CT", "Before buffer reader");
                Log.v("CLIENTTASK", "I am "+ my_node_id +" my Predecessor is "+ pred_hash_id +" my Successor is "+ succ_hash_id);
                //socket.close();
            } catch (UnknownHostException e) {
                Log.v("CLIENTTASK EXCEPTION", "UnknownHostException");
            } catch (IOException e) {
                Log.v("CLIENTTASK EXCEPTION", "socketIOException");
            }
            Log.v("CLIENTTASK", "Last Line");
            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.v("SERVERTASK()", "...................................................");
            ServerSocket serverSocket = sockets[0];
            try{
                while(true)
                {
                    //Log.v("ST", "1");
                    Socket sock = serverSocket.accept();
                    //Log.v("ST", "2");
                    BufferedReader br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                    //Log.v("ST", "3");
                    String ms = br.readLine();
                    //Log.v("ST", "4");
                    String[] messages = ms.split(myDelimiter);
                    // { msgKey/port, msgValue, hash_value, msgType }
                    Log.v("SERVERTASK", "Received message "+ messages[0]);
                    String  msg_key, msg_value, hash_value, msg_type;
                    msg_type = messages[0];

                    switch(msg_type){

                        case INSERT:

                            String key_d = messages[1];
                            String value_d = messages[2];
                            Log.v("SERVERTASK", "INSERT CASE");
                            Log.v("SERVERTASK", "INSERT KEY "+key_d+" VALUE"+value_d);
                            FileOutputStream out_stream = getContext().openFileOutput(key_d, Context.MODE_PRIVATE);
                            out_stream.write(value_d.getBytes());
                            out_stream.close();
                            //sendMsg(messass, toSend);
                            String messa11 = INSERTED+myDelimiter+INSERTED;
                            PrintWriter pw112 = new PrintWriter(sock.getOutputStream(), true);
                            pw112.println(messa11);
                            Log.v("SERVERTASK", "Printwrote messages "+ messa11);
                            break;

                        case QUERY:
                            String key = messages[1];
                            String sender = messages[2];
                            Log.v("SERVERTASK","QUERY CASE, KEY "+key);
                            File file = new File(getContext().getFilesDir(),key);
                            if(file.exists())
                            {
                                String sb="";
                                try {
                                    int size;

                                    FileInputStream in_stream = getContext().openFileInput(key);
                                    while ((size = in_stream.read()) != -1) {
                                        sb += (char) size;
                                        //sb+= Character.toString((char) size);
                                    }
                                    //Log.v("inside query 1","sb"+sb);
                                    in_stream.close();
                                } catch (FileNotFoundException e) {
                                    Log.e("SERVERTASK EXCEPTION ","FNE");
                                } catch (IOException e) {
                                    Log.e("SERVERTASK EXCEPTION ","IOE");
                                } catch (NullPointerException e) {
                                    Log.e("SERVERTASK EXCEPTION ","NPE");
                                }
                                String message1 = QUERY_REPLY+myDelimiter+key+myDelimiter+sb+myDelimiter
                                        +my_port_id;
                                PrintWriter pwsss1 = new PrintWriter(sock.getOutputStream(), true);
                                pwsss1.println(message1);
                            }
                            else
                            {
                                Log.v("QUERY", "Not There value");
//                                String message1 = QUERY+myDelimiter+key+myDelimiter+sender;
//                                sendMsg(message1, succ_port_id);
                            }
                            break;

                        case QUERY_ALL:
                            String key1 = messages[1];
                            String sender1 = messages[2];
                            Log.v("SERVERTASK","QUERY_ALL CASE, KEY "+key1);
                            if(my_port_id.compareTo(sender1)==0)
                            {
                                foundOrigin = true;

                            }
                            else
                            {
                                String mess = QUERY_ALL_REPLY+myDelimiter;
                                try{
                                    BufferedReader reader = null;
                                    String s = "";
                                    //Log.v("0", "file reading");
                                    File file_path = new File(getContext().getFilesDir().getAbsolutePath());

                                    if (file_path.isDirectory()) {
                                        String[] filesy = file_path.list();
                                        int no_of_files = filesy.length;
                                        if (no_of_files > 0) {
                                            if (file_path.listFiles() != null) {
                                                for (File f : file_path.listFiles()) {
                                                    s = f.getName();
                                                    reader = new BufferedReader(new FileReader(file_path + "/" + s));
                                                    String value1 = reader.readLine();
                                                    String[] valss = value1.split(delim);
                                                    value1 = valss[0];
                                                    String mshg = s + myDelimiter + value1 + myDelimiter;
                                                    mess = mess + mshg;
                                                }
                                                reader.close();

                                            }
                                        }
                                    }
                                    sendMsg(mess, sender1);
                                    break;
                                } catch (Exception e) {
                                    Log.e("SERVERTASK", "file reading has failed"+e.getStackTrace());
                                }
                            }
                            break;

                        case QUERY_REPLY:
                            String key2 = messages[1];
                            String value2 = messages[2];
                            Log.v("SERVERTASK","QUERY REPLY, KEY: "+ key2 +"VALUE: "+value2);
                            single_query.put(key2, value2);
                            break;

                        case QUERY_ALL_REPLY:
                            int len = messages.length;
                            responseNum ++;
                            if(len > 1)
                            {
                                int i = 1;
                                while(i<len)
                                {
                                    String key_1 = messages[i];
                                    String value_1 = messages[i+1];
                                    i = i+2;
                                    star_queries.put(key_1,value_1);
                                    Log.v("SERVERTASK", "QUERY_ALL_REPLY "+"key : "+key_1+" value : "+value_1);
                                }
                            }
                            break;

                        case DELETE:
                            String sender2 = messages[2];
                            if(my_port_id.compareTo(sender2)==0)
                            {
                                //Log.v("1", "file deleting");
                                deleteSingle = true;
                                //break;
                            }
                            String del_key = messages[1];
                            File file1 = new File(getContext().getFilesDir(),del_key);
                            if(file1.exists())
                            {
                                if(file1.delete()){
                                    //Log.v("3", "file deleting");
                                    Log.v("SERVERTASK","DELETE, KEY: "+ del_key);
                                }
                            }
                            else
                            {
                                Log.v("SERVERTASK","HAS NO DELETE, KEY: "+ del_key);
                                // Log.v("SERVERTASK","SENDING MSG TO: "+ sender2);
                                //  String message = DELETE+myDelimiter+del_key+myDelimiter+sender2;
                                //sendMsg(message, succ_port_id);
                                //Log.v("4", "file deleting");
                            }
                            break;

                        case DELETE_ALL:
                            String sender3 = messages[2];
                            // if(my_port_id.compareTo(sender3)==0)
                            // {
                            //     deleteAll= true;
                            // }
                            int count = 0;
                            String del_key1 = messages[1];
                            try {
                                BufferedReader reader = null;
                                File file_path = new File(getContext().getFilesDir().getAbsolutePath());
                                if (file_path.listFiles() != null) {
                                    for (File f : file_path.listFiles()) {
                                        if(f.delete())
                                        {
                                            count ++;
                                        }
                                    }
                                    //Log.v("6", "file deleting");
                                    reader.close();
                                }
                                Log.v("SERVERTASK, DELETE_ALL", "files are deleting");
                            } catch (Exception e) {
                                Log.e("SERVERTASK","DELETE ALL EXCEPTIO"+ "file deleting has failed");
                            }
                            String message = DELETE_ALL_REPLY+myDelimiter+del_key1+myDelimiter+sender3;
                            Log.v("SERVERTASK, DELETE_ALL", "Sending deleted all reply to "+sender3);
                            sendMsg(message, sender3);
                            break;

                        case DELETE_ALL_REPLY:
                            Log.v("SERVERTASK"," DELETE_ALL_REPLY");
                            delResponse++;
                            break;

                        case GETMINE:
                            Log.v("SERVERTASK", " GETMINE");
                            String toSend = messages[1];
                            String messass = GETMINE_REPLY+myDelimiter;
                            try{
                                BufferedReader reader = null;
                                String s = "";
                                //Log.v("0", "file reading");
                                File file_path = new File(getContext().getFilesDir().getAbsolutePath());

                                if (file_path.isDirectory()) {
                                    String[] filesy = file_path.list();
                                    int no_of_files = filesy.length;
                                    if (no_of_files > 0) {
                                        if (file_path.listFiles() != null) {
                                            for (File f : file_path.listFiles()) {
                                                s = f.getName();
                                                reader = new BufferedReader(new FileReader(file_path + "/" + s));
                                                String value12 = reader.readLine();
                                                String mshgs = s + myDelimiter + value12 + myDelimiter;
                                                messass = messass + mshgs;
                                            }
                                            reader.close();

                                        }
                                    }
                                }
                                //sendMsg(messass, toSend);
                                PrintWriter pw11 = new PrintWriter(sock.getOutputStream(), true);
                                pw11.println(messass);
                                Log.v("SERVERTASK, GETMINE", "Sending message"+messass);
                                break;
                            } catch (Exception e) {
                                Log.e("SERVERTASK EXCEPTION", "file reading has failed"+e.getStackTrace());
                            }
                            break;

                        // case GETMINE_REPLY:
                        //     Log.v("GETMINE_REPLY", "in GETMINE_REPLY");
                        //     try{
                        //         int lens= messages.length;
                        //         if(lens > 1)
                        //         {
                        //             int i = 1;
                        //             while(i<lens)
                        //             {
                        //                 String key_1 = messages[i];
                        //                 String value_1 = messages[i+1];
                        //                 String key1_hash = genHash(key_1);
                        //                 String port_vals = getPortToInsert(key1_hash);
                        //                 i = i+2;
                        //                 if((port_vals.compareTo(pred_port_id)==0) || (port_vals.compareTo(my_port_id)==0)
                        //                         || (port_vals.compareTo(pred_port_id2)==0))
                        //                 {
                        //                     //insert into current avd
                        //                     FileOutputStream out_stream_1= getContext().openFileOutput(key_1, Context.MODE_PRIVATE);
                        //                     out_stream_1.write(value_1.getBytes());
                        //                     out_stream_1.close();
                        //                 }
                        //
                        //             }
                        //         }
                        //         getThreeResponse++;
                        //     }
                        //     catch(NoSuchAlgorithmException e){}
                        //     break;
                    }
                    //printTreeMap();
                }

            }catch(IOException e){
                Log.e("SERVERTASK", "IOException ");
            }
            catch(NullPointerException e){
                Log.e("SERVERTASK", "Null Pointerr Exception ");
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {


        }
    }

}
