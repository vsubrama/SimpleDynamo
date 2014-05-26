package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Map;
import java.util.Map.Entry;


import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	
	private MainDatabaseHelper mOpenHelper;
	public Context mContext;
	public Uri mUri;
    
	private Uri buildUri(String scheme, String authority) {
		Log.v("Build uri called", scheme + " " + authority);
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		synchronized(Globals.Lock)
		{
		int delete = 0;
		//Log.v("Delete called at " + Globals.myPort," ");
    	SQLiteDatabase db;
    	db = mOpenHelper.getWritableDatabase();
    	if (selection.equals("*"))
    	{
    		synchronized(Globals.MessagesList)
			{
    		Globals.MessagesList.clear();
			}
    		delete = db.delete("Message_Dynamo", null, null);
    	}
    	else if (selection.equals("@"))
    	{
    		synchronized(Globals.MessagesList)
			{
    		Globals.MessagesList.clear();
			}
    		delete = db.delete("Message_Dynamo", null, null);
    	}
    	else 
    	{
    		//db.delete(DATABASE_TABLE, KEY_NAME + "=" + name, null)
    		//Log.v("Delete Specific at "+ Globals.myPort, selection);
    		synchronized(Globals.MessagesList)
			{
    		Globals.MessagesList.clear();
			}
    		delete = db.delete("Message_Dynamo", null, null);
    		virtualMessage mess = new virtualMessage(Globals.myPort,null,selection,null,"delete",null);
    		ArrayList<String> succlist = new ArrayList<String>();
    		succlist.add(Globals.succ1node);
    		succlist.add(Globals.succ2node);
    		for (String S: succlist)
    		{
    			mess.to = S;
	    		Client temp = new Client(mess);
	    		Thread temp1  = new Thread(temp);
	    		temp1.start();
	    		try {
					temp1.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
				e.printStackTrace();
				}
    		}
    		/*Globals.MessagesList.remove(selection);
    		delete = db.delete("Message_Dynamo", "key=?", new String[] { selection });
    		if(selectionArgs == null)
    		{
    			Log.v("Delete selection args is null " + Globals.myPort, "This should be deleted");
	    		virtualMessage mess = new virtualMessage(Globals.myPort,null,selection,null,"delete",null);
	    		ArrayList<String> succlist = new ArrayList<String>();
	    		succlist.add(Globals.succ1node);
	    		succlist.add(Globals.succ2node);
	    		for (String S: succlist)
	    		{
	    			mess.to = S;
		    		Client temp = new Client(mess);
		    		Thread temp1  = new Thread(temp);
		    		temp1.start();
		    		try {
						temp1.join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
					e.printStackTrace();
					}
	    		}
    		}*/
    	}
		
        Log.v("Delete", selection);
        return delete;
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		synchronized(Globals.Lock)
		{
			 while (Globals.recoveryFirst)
			 {
			    try {
					Globals.Lock.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			 }
		SQLiteDatabase db;
		if (Globals.RecoveryLock==true)
		{
			//Log.v("First insert called at " + Globals.myPort, "Recovering message at db");
			db = mOpenHelper.getReadableDatabase();
			Cursor c;
			c =  db.rawQuery("SELECT * FROM Message_Dynamo ", null);
			while (c.isAfterLast() == false) 
			{
				//Log.v("Copying data to messagelist at "+ Globals.myPort,c.getString(0) + " " + c.getString(1));
				ArrayList<String> al = new ArrayList<String>();
				al.add(c.getString(1));
				al.add(c.getString(2));
				synchronized(Globals.MessagesList)
				{
				Globals.MessagesList.put(c.getString(0),al);
				}
				//sm.messDump.put(c.getString(0),c.getString(1));
				c.moveToNext();
			}
			Globals.RecoveryLock = false;
		}
    	db = mOpenHelper.getWritableDatabase();
    	String hKey = (String) values.get("key");
    	String hValue = (String) values.get("value");
    	String hType =  (String) values.get("type");
    	String horigport = (String)values.get("origport");
    	Log.v("Insert called on " + Globals.myPort," ");
	    	if (Globals.Alivelist.size()==1)
	    	{
	    		ContentValues cv = new ContentValues();
	    		cv.put("key", (String) values.get("key"));
	    		cv.put("value", (String) values.get("value"));
	    		cv.put("origport", Globals.myPort);
	    		db.insertWithOnConflict("Message_Dynamo", null, cv, SQLiteDatabase.CONFLICT_REPLACE );
	    		ArrayList<String> ar= new ArrayList<String>();
	    		ar.add(hValue);
	    		ar.add(horigport);
	    		synchronized(Globals.MessagesList)
	    		{
	    		Globals.MessagesList.put(hKey, ar);
	    		}
	    	}
	    	else if (hType!=null && hType.equalsIgnoreCase("Message"))
	    	{
	    		//Log.v("Reached Message at " + Globals.myPort, (String) values.get("key") + (String) values.get("value"));
	    		ContentValues cv = new ContentValues();
	    		cv.put("key", hKey);
	    		cv.put("value", hValue);
	    		cv.put("origport", horigport);
	    		db.insertWithOnConflict("Message_Dynamo", null, cv, SQLiteDatabase.CONFLICT_REPLACE );
	    		ArrayList<String> ar= new ArrayList<String>();
	    		ar.add(hValue);
	    		ar.add(horigport);
	    		synchronized(Globals.MessagesList)
	    		{
	    		Globals.MessagesList.put(hKey, ar);
	    		}
	    		Log.v("Final Insert  " + Globals.myPort,hKey + " " +hValue);
	    	}
	    	else
	    	{
	    		Log.v("Reached M part at insert " + Globals.myPort, hKey + " "+ hValue);
	    		ArrayList<String> runMinnode =new ArrayList<String>();
	    		for (int i=0;i<Globals.portStrHashlist.size();i++)
				{					
					if (genHashWrapper(hKey).compareTo(genHashWrapper(Globals.portStrHashlist.get(i))) >0 && 
							genHashWrapper(hKey).compareTo(genHashWrapper(Globals.portStrHashlist.get((i + 1)%Globals.portStrHashlist.size()))) < 0)
					{
						runMinnode.add(Globals.portStrHashlist.get((i + 1)%Globals.portStrHashlist.size()));
						runMinnode.add(Globals.portStrHashlist.get((i + 2)%Globals.portStrHashlist.size()));
						runMinnode.add(Globals.portStrHashlist.get((i + 3)%Globals.portStrHashlist.size()));
						break;
					}
				}
				if (runMinnode.isEmpty())
				{
					runMinnode.add(Globals.portStrHashlist.get(0));
					runMinnode.add(Globals.portStrHashlist.get(1));
					runMinnode.add(Globals.portStrHashlist.get(2));
				}
				/*Log.v("Key at " + Globals.myPort, hKey + " Should be inserted in " + runMinnode.get(0));
				for (int i=0;i<Globals.portStrHashlist.size();i++)
				{
					Log.v("Iterating through portstrhashlist at " + Globals.myPort, Globals.portStrHashlist.get(i));
					Log.v("Iterating through runMinnode at " + Globals.myPort, runMinnode.get(i));
				}*/
				for (int i=0;i<runMinnode.size();i++)
				{
					runMinnode.set(i,  String.valueOf((Integer.parseInt(runMinnode.get(i)) * 2)));
					//new virtualMessage(Globals.myPort,null,hKey,hValue,"M",runMinnode.get(0));
		    		virtualMessage mess = new virtualMessage(Globals.myPort,null,hKey,hValue,"M",runMinnode.get(0));
		    		mess.to = runMinnode.get(i);
		    		mess.OriginalInsert = runMinnode.get(0);
		    		if (mess.to.equalsIgnoreCase(Globals.myPort))
		    		{
		    			ContentValues cv = new ContentValues();
		    			cv.put("key", hKey);
			    		cv.put("value", hValue);
			    		cv.put("origport", runMinnode.get(0));
			    		db.insertWithOnConflict("Message_Dynamo", null, cv, SQLiteDatabase.CONFLICT_REPLACE );
			    		ArrayList<String> ar= new ArrayList<String>();
			    		ar.add(hValue);
			    		ar.add(runMinnode.get(0));
			    		synchronized(Globals.MessagesList)
			    		{
			    		Globals.MessagesList.put(hKey, ar);
			    		}
			    		Globals.inSucess.decrementAndGet();
			    		continue;
		    		}
		    		Client temp = new Client(mess);
		    		Thread temp1  = new Thread(temp);
		    		temp1.start();
		    		try {
						temp1.join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
					e.printStackTrace();
					}
				}
				/*while(Globals.inSucess.get()!=0);
				Globals.inSucess.set(3);*/
				//Log.v("Breaking out of blocking loop " + Globals.myPort, " for " + hKey+ " " + hValue);
	    	}
		return uri;
		}
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel;
    	mContext = getContext();
        tel = (TelephonyManager) mContext.getSystemService(Context.TELEPHONY_SERVICE);
    	mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		mOpenHelper = new MainDatabaseHelper(
			mContext,        // the application context
            null,              // the name of the database)
            null,                // uses the default SQLite cursor
            1                    // the version number
				);
		Log.v("Inside Oncreate after building uri","");
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        Globals.portStr = portStr;
        Globals.myPort = String.valueOf((Integer.parseInt(portStr) * 2));        
        try 
        {
			Globals.mynodeid = genHash(Globals.myPort);
		} 
        catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Log.v("Inside Oncreate before server creation","");
        ServerSocket serverSocket = null;
       /* try {*/
        	virtualMessage mess = new virtualMessage(Globals.myPort,null,null,null,"Join",null);
            Client temp = new Client(mess);
    		Thread t = new Thread(temp);
    		t.start();
    		try {
				t.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				Log.e("Intruppted exception","");
			}
    		Log.v("Client spawn done","");
			try {
				serverSocket = new ServerSocket (10000);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				Log.e("IOException exception"," ");
				e.printStackTrace();
			}
			new Server(mContext).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR , serverSocket);
			//new Thread(new Server(mContext,serverSocket)).start();
			Log.v("Inside Oncreate after server creation","");
			//edu.buffalo.cse.cse486586.simpledynamo.provider
		
       return false;
	} 
	

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		
		
		synchronized (Globals.Lock) {
			 while (Globals.recoveryFirst)
			 {
			    try {
					Globals.Lock.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			 }
		Cursor c = null;
    	//MatrixCursor c1 = new MatrixCursor(new String[]{"key","value"});
    	SQLiteDatabase db;
    	if (Globals.RecoveryLock==true)
		{
    		//Log.v("First query called at " + Globals.myPort, "Recovering message at db");
			db = mOpenHelper.getReadableDatabase();
			Cursor c1;
			c1 =  db.rawQuery("SELECT * FROM Message_Dynamo ", null);
			while (c1.isAfterLast() == false) 
			{
				//Log.v("Copying data to messagelist at "+ Globals.myPort,c1.getString(0) + " " + c1.getString(1));
				ArrayList<String> al = new ArrayList<String>();
				al.add(c1.getString(1));
				al.add(c1.getString(2));
				synchronized(Globals.MessagesList)
				{
				Globals.MessagesList.put(c1.getString(0),al);
				}
				//sm.messDump.put(c.getString(0),c.getString(1));
				c1.moveToNext();
			}
			Globals.RecoveryLock = false;
		}
    	db = mOpenHelper.getReadableDatabase();
    	
    	if (selection.equals("*"))
    	{
    		//Change this later used for debugging
    	//	Log.v("Query called :) ", " ");
    		if (Globals.Alivelist.size() == 1)
    		{
    			c =  db.rawQuery("SELECT key,value FROM Message_Dynamo", null);
    			 c.setNotificationUri(mContext.getContentResolver(), uri);
    		        Log.v("query", selection);
    				return c;
    		}
    		else
    		{
    			//Log.v("Global Dump called " + Globals.myPort, "  ");
    			final virtualMessage mess = new virtualMessage(Globals.myPort,null,null,null,"globaldump",null);
    		    //new Client().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mess);
    		    Client temp = new Client(mess);
	    		Thread tt = new Thread(temp);
	    		tt.start();
	    		try {
					tt.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		     //this line will execute immediately, not waiting for your task to complete    			
    			while(Globals.GdumpFlag);
    			//Log.e("Busy wait looped out"," at " + Globals.myPort);
    			MatrixCursor c1 = new MatrixCursor(new String[]{"key","value"});
    			/*for (Map.Entry<String, ArrayList<String>> entry : Globals.gDumpMessagesList.entrySet())
				{
					String key1 = entry.getKey();
				    ArrayList<String> value = entry.getValue();
				    String Value1 = value.get(0);
				    String 
					sm.messDump.put(key1,value.get(0));
				}*/
    			for (Map.Entry<String,String> entry : Globals.gDumpMessagesList.entrySet())
				{
					Log.v("Global Dump Achieved " + Globals.myPort, entry.getKey() + " " + entry.getValue());
					c1.addRow(new String[]{ entry.getKey().toString(),entry.getValue().toString()});
				}
    			c1.close();
    			Globals.gDumpMessagesList.clear();
    			Globals.GdumpFlag = true;
    			c1.setNotificationUri(mContext.getContentResolver(), uri);
    			return c1;
    		}
    	}
    	else if (selection.equals("@"))
    	{
    		c =  db.rawQuery("SELECT key,value FROM Message_Dynamo ", null);
    		int i=0;
    		synchronized(Globals.MessagesList)
			{
    		for (Entry<String, ArrayList<String>> entry : Globals.MessagesList.entrySet())
			{
    			i++;
    			ArrayList<String> arr = entry.getValue();
				Log.v("Iterating through Globals.MessagesList " + Globals.myPort, entry.getKey() + " " + arr.get(0) + " " + arr.get(1) +" "+ i);
			}
			}
    		if (c == null)Log.v("Returned no @ rows on ", Globals.myPort);
    		 c.setNotificationUri(mContext.getContentResolver(), uri);
    	        Log.v("query", selection);
    			return c;
    	}
    	else if (selection.equals("$"))
    	{
    		c =  db.rawQuery("SELECT * FROM Message_Dynamo ", null);
    		 c.setNotificationUri(mContext.getContentResolver(), uri);
    	        Log.v("query", selection);
    			return c;
    	}
    	else 
    	{
    		Log.v("Entered Specific at " + Globals.myPort,selection);
    		ArrayList<String> sQuerValu = new ArrayList<String>(); 
    		synchronized(Globals.MessagesList)
			{
    		sQuerValu =	Globals.MessagesList.get(selection);
			}
    		if (sQuerValu==null)
    		{
    			Log.v("Spawning thread at " + Globals.myPort," No value found at local ");
    			final virtualMessage mess = new virtualMessage(Globals.myPort,null,selection,null,"specific",null);
    		    //new Client().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mess);
    		    Client temp = new Client(mess);
	    		Thread tt = new Thread(temp);
	    		tt.start();
	    		try {
					tt.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    		while(Globals.GdumpFlag);
	    		Log.v("Breaking out of specific wait loop at " + Globals.myPort, " for " + selection);
	    		MatrixCursor c1 = new MatrixCursor(new String[]{"key","value"});
	    		for (@SuppressWarnings("rawtypes") Map.Entry entry : Globals.gDumpMessagesList.entrySet())
				{
					Log.v("Specific Key found " + Globals.myPort, entry.getKey() + " " + entry.getValue());
					c1.addRow(new String[]{ entry.getKey().toString(),entry.getValue().toString()});
				}
    			c1.close();
    			Globals.gDumpMessagesList.clear();
    			Globals.GdumpFlag = true;
    			c1.setNotificationUri(mContext.getContentResolver(), uri);
    			return c1;
    		}
    		else
    		{
    			MatrixCursor c1 = new MatrixCursor(new String[]{"key","value"});
    			c1.addRow(new String[]{selection,sQuerValu.get(0)});
    			c1.setNotificationUri(mContext.getContentResolver(), uri);
    			Log.v(" Selection found and returning at " + Globals.myPort, selection + sQuerValu);
    			return c1;
    		}
		}
		}
	}
	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        @SuppressWarnings("resource")
		Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    public String genHashWrapper(String input)
    {
    	String rValue = null;
    	try {
			rValue = genHash(input);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return rValue;
    }
}
