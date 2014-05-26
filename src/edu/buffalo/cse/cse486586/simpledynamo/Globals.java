package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Globals {
	public static volatile String myPort;
	public static volatile String mynodeid;
	public static volatile String portStr;
	//public static String successor;
	//public static String predecessor;
	public static volatile boolean GdumpFlag = true;
	public static volatile ArrayList<String> Alivelist = new ArrayList<String>();
	public static volatile ArrayList<String> portStrAlivelist = new ArrayList<String>();
	//Sorted port list for hash insert
	public static volatile ArrayList<String> portStrHashlist = new ArrayList<String>();
	public static volatile LinkedHashMap<String,ArrayList<String>> MessagesList = new LinkedHashMap<String,ArrayList<String>>();
	
	//public static ArrayList<String> gDumpAlivelist = new ArrayList<String>();
	public static volatile LinkedHashMap<String,String> gDumpMessagesList = new LinkedHashMap<String,String>();
	//experimental
	//public static CountDownLatch GDumpMessallocated = new CountDownLatch(1/*wait for one signal*/);
	public static boolean Glock = true; 
	public static boolean RecoveryLock = true;
	public static boolean deleteFlag = false;
	public static boolean recoveryFirst = true;
	//
	public static volatile String prev2node = null;
	public static volatile String prev1node = null;
	public static volatile String succ1node = null;
	public static volatile String succ2node = null;
	public static volatile AtomicInteger inSucess= new AtomicInteger(3);
	public static volatile Object Lock = new Object();
	public static volatile Object recoveryFirstLock = new Object();
	public static volatile AtomicInteger recSucess= new AtomicInteger(4);
	public static volatile AtomicInteger rec1st= new AtomicInteger(0);
	
}
