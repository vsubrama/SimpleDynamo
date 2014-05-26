package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;

import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
public class Server extends AsyncTask<ServerSocket, String, Void> {
	public Context mContext;
	public Uri mUri;
	public Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	public Server(Context mContext)
	{
		this.mContext = mContext;
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	}
	protected Void doInBackground(ServerSocket... sockets) {
		ServerSocket serverSocket = sockets[0];
		Log.v("Server is spawning ", " fuck me ");
		Socket socket=null;
		virtualMessage mess = new virtualMessage(null,null,null,null,"Recovery",null);
		synchronized (Globals.Lock) 
		{
			if (Globals.rec1st.get() > 0)
			{
				Client recoveryThread = new Client(mess);
				Thread tl = new Thread(recoveryThread);
				tl.start();
				try {
					tl.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				//while(Globals.RecoveryLock);
			}
			Globals.recoveryFirst=false;
			Globals.Lock.notify();
			Log.v(" Recovery count at " + Globals.myPort," decrement by one to get number of recoveries " + Globals.rec1st.get());
			Globals.rec1st.incrementAndGet();
		 }
		try
		{
			while(true)
			{
				Log.v("Into while loop we go", "");
				virtualMessage sm = null;
				try
				{
					Log.v("Server ready to read", "");
					socket = serverSocket.accept();
					//Log.v("Server Created",Globals.myPort);
					ObjectInputStream nw = new ObjectInputStream(socket.getInputStream());
					//nw.reset();
					sm = (virtualMessage) nw.readObject();
					nw.close();
					socket.close();
				} 
				catch (IOException e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
				catch (ClassNotFoundException e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (sm !=null)
				{
				Log.v("REceived at server " + Globals.myPort, " " + sm.from + " " + sm.to + " " + sm.key + " "+sm.value+" "+sm.type);
				if (sm.to.equals(Globals.myPort))
				{
					if (sm.type.equalsIgnoreCase("join"))
					{
						//Log.v("AliveList size  " + Globals.Alivelist.size(), " Client list size " + Integer.parseInt(sm.key.trim()));
						if (!Globals.Alivelist.contains(sm.from))
						{
							//new Client().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,Globals.myPort,Globals.portStr,"Join");
							Globals.Alivelist.add(sm.from);
							Globals.portStrAlivelist.add(sm.value);
							//Log.v(sm.from," Joined " + " at " + Globals.myPort);
						}
						
					}
				else if (sm.type.equalsIgnoreCase("M"))
					{
						ContentValues cv = new ContentValues();
						cv.put("key",sm.key);
						cv.put("value",sm.value);
						cv.put("type", "message");
						cv.put("origport", sm.OriginalInsert);
						mContext.getContentResolver().insert(mUri, cv);
						virtualMessage vm = new virtualMessage(Globals.myPort, sm.from, sm.key, sm.value, "inSucess",null);
						Client temp = new Client(vm);
			    		Thread t = new Thread(temp);
			    		t.start();
			    		t.join();
					}
				else if (sm.type.equalsIgnoreCase("globaldump"))
					{
						if (sm.GlobalmessDumpNodelist.contains(Globals.myPort))
						{
							/*Log.e("Final stage of Gdump", "at " + Globals.myPort);
							for (Map.Entry<String,String> entry : sm.messDump.entrySet())
							{
								Log.v("Iterating through messages dump " + Globals.myPort, entry.getKey() + " " + entry.getValue());
								//c1.addRow(new String[]{ entry.getKey().toString(),entry.getValue().toString()});
							}*/
							Globals.gDumpMessagesList.clear();
							Globals.gDumpMessagesList = sm.messDump;
							Globals.GdumpFlag = false;
						}
						else
						{
							Log.e("Gdump message received spawning client to handle it", "at " + Globals.myPort);
							//new Client().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,sm);
							/*for (Map.Entry<String,String> entry : sm.messDump.entrySet())
							{
								Log.v("Iterating through messages dump " + Globals.myPort, entry.getKey() + " " + entry.getValue());
								//c1.addRow(new String[]{ entry.getKey().toString(),entry.getValue().toString()});
							}*/
							Client temp = new Client(sm);
				    		Thread tt = new Thread(temp);
				    		tt.start();
				    		tt.join();
						}
					}
					else if (sm.type.equalsIgnoreCase("specific"))
					{
						if (sm.GlobalmessDumpNodelist.contains(Globals.myPort))
						{
							//Log.e("Final stage of Specific", "at " + Globals.myPort);
							Globals.gDumpMessagesList = sm.messDump;
							Globals.GdumpFlag = false;
						}
						else
						{
							//Log.e("Specific message received spawning client to handle it", "at " + Globals.myPort);
							Client temp = new Client(sm);
				    		Thread tt = new Thread(temp);
				    		tt.start();
				    		tt.join();
						}
					}
					else if (sm.type.equalsIgnoreCase("RecoveryPrev"))
					{
						synchronized(Globals.Lock)
						{
							Log.v("REceived Recovery predessor message at "+ Globals.myPort,"from " + sm.from);
							//String key = "@";
							//Cursor c = mContext.getContentResolver().query(mUri, null,key, null, null);
							
							//c.moveToFirst();
							/*while (c.isAfterLast() == false) 
							{
								//Log.v("Recovery predessor at "+ Globals.myPort,c.getString(0) + " " + c.getString(1));
								sm.messDump.put(c.getString(0),c.getString(1));
								c.moveToNext();
							}*/
							synchronized(Globals.MessagesList)
							{
								if (Globals.MessagesList.isEmpty())Log.v("Empty cursor at " + Globals.myPort," RecoverySucc FML ");
								for (Map.Entry<String, ArrayList<String>> entry : Globals.MessagesList.entrySet())
								{
									String key1 = entry.getKey();
								    ArrayList<String> value = entry.getValue();
									sm.messDump.put(key1,value.get(0));
								}
							}
							sm.to = sm.from;
							sm.from = Globals.myPort;
							//sm.key = sm.to;
							sm.type = "Recovery";
							Client temp = new Client(sm);
				    		Thread t = new Thread(temp);
				    		t.start();
				    		t.join();
						}						
					}
					else if (sm.type.equalsIgnoreCase("RecoverySucc"))
					{
						synchronized(Globals.Lock)
						{
							Log.v("REceived Recovery Successor message at "+ Globals.myPort,"from " + sm.from);
							//String key = "$";
							//Cursor c = mContext.getContentResolver().query(mUri, null,key, null, null);
							
							//c.moveToFirst();
							/*while (c.isAfterLast() == false) 
							{
								if (c.getString(2).equalsIgnoreCase(sm.from))
								{
									//Log.v("Recovery predessor at "+ Globals.myPort,c.getString(0) + " " + c.getString(1));
									sm.messDump.put(c.getString(0),c.getString(1));
								}
								c.moveToNext();
							}*/
							synchronized(Globals.MessagesList)
							{
								if (Globals.MessagesList.isEmpty())Log.v("Empty cursor at " + Globals.myPort," RecoverySucc FML ");
								for (Map.Entry<String, ArrayList<String>> entry : Globals.MessagesList.entrySet())
								{
									String key1 = entry.getKey();
								    ArrayList<String> value = entry.getValue();
								    if(value.get(1).equalsIgnoreCase(sm.from))
								    {
									sm.messDump.put(key1,value.get(0));
								    }
								}
							}
							sm.to = sm.from;
							sm.from = Globals.myPort;
							//sm.key = sm.to;
							sm.type = "Recovery";
							Client temp = new Client(sm);
				    		Thread t = new Thread(temp);
				    		t.start();
				    		t.join();
						}
					}
					else if (sm.type.equalsIgnoreCase("Recovery"))
					{
						synchronized(Globals.Lock)
						{
							ArrayList<String> ptr = new ArrayList<String>();
							Log.v("REceived Recovery dta dump message at "+ Globals.myPort,"from " + sm.from);
							if(sm.messDump.isEmpty())Log.v("Empty Message dump  at " + Globals.myPort," Recovery FML ");
							for (Map.Entry<String,String> entry : sm.messDump.entrySet())
							{
								Log.v("Iterating through received data at " + Globals.myPort, entry.getKey().toString() + " " +entry.getValue().toString());
								ContentValues cv = new ContentValues();
								cv.put("key",entry.getKey().toString());
								cv.put("value",entry.getValue().toString());
								if(sm.from.equalsIgnoreCase(Globals.succ1node) || sm.from.equalsIgnoreCase(Globals.succ2node))
								{
									cv.put("origport", Globals.myPort);
									ptr.add(sm.value);
									ptr.add(Globals.myPort);
								}
								if(sm.from.equalsIgnoreCase(Globals.prev1node) || sm.from.equalsIgnoreCase(Globals.prev2node))
								{
									cv.put("origport", sm.from);
									ptr.add(sm.value);
									ptr.add(sm.from);
								}
								cv.put("type", "Message");
								mContext.getContentResolver().insert(mUri, cv);
							}
							Globals.recSucess.decrementAndGet();
							if(Globals.recSucess.get() == 0)Globals.RecoveryLock = false;
						}
					}
					else if (sm.type.equalsIgnoreCase("inSucess"))
					{
						
						Globals.inSucess.decrementAndGet();
						//Log.v("inSucess message received at "+ Globals.myPort,"for "+sm.key + " " + sm.value + " " + " inSucess value now " + Globals.inSucess.get());
					}
					else if (sm.type.equalsIgnoreCase("delete"))
					{
						mContext.getContentResolver().delete(mUri, "@",new String[]{"aaaa"});
						Log.v("Delete message received at "+ Globals.myPort,"for "+sm.key + " " );
					}
				}
			}
				
			}
		}
		catch (Exception Ex)
		{
			Ex.printStackTrace();
			Log.d("Couldn't create server","Failed");
		}
		return null;
	}


	protected void onProgressUpdate(String... strings ){
	
		return;
	}
	

}