package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import android.util.Log;


public class Client implements Runnable {
	//public ArrayList<String> portlist = new ArrayList<String>();
	SimpleDynamoProvider db = new SimpleDynamoProvider();
	virtualMessage vm;
	public Client(virtualMessage mess)
	{
		vm = mess;
	}
		@Override
		public void run() {
			Log.v("client spawned messages received ", vm.type + " " + vm.key + " " + vm.value);
			try{
				if(vm.type.equalsIgnoreCase("join"))
				{
					Log.v("Inside Client", "Going to send Join message" + "  " + Globals.myPort);
					Globals.Alivelist.add("11108");
					Globals.Alivelist.add("11112");
					Globals.Alivelist.add("11116");
					//Globals.Alivelist.add("11120");
					//Globals.Alivelist.add("11124");
					Globals.portStrAlivelist.add("5554");
					Globals.portStrAlivelist.add("5556");
					Globals.portStrAlivelist.add("5558");
					//Globals.portStrAlivelist.add("5560");
					//Globals.portStrAlivelist.add("5562");
					ArrayList<String> temp = new ArrayList<String>();
					//ArrayList<String> tFlist = new ArrayList<String>();
					for (int i=0;i<Globals.portStrAlivelist.size();i++)
					{
						
						temp.add(db.genHashWrapper(Globals.portStrAlivelist.get(i)));
					}
					Collections.sort(temp);
					for(String hash : temp)
					{
						for (String node : Globals.portStrAlivelist)
						{
							if (hash.equalsIgnoreCase(db.genHashWrapper(node)))
							{
								Globals.portStrHashlist.add(node);							
							}
						}
						
					}
					/*for (String S : Globals.portStrHashlist)
					{
						Log.v("Port String hash list at " + Globals.myPort , S + Globals.portStrHashlist.size());
					}*/
					Log.v("Inside Client", "   Join Message sent");
				}
				else if (vm.type.equalsIgnoreCase("M"))
				{
					try {
						Log.v("Client spawned at " + Globals.myPort,"Message being sent " + vm.from + vm.to+vm.key+vm.value +vm.type);
						Socket sendSock =  new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(vm.to));
						ObjectOutputStream nw = new ObjectOutputStream(sendSock.getOutputStream());
						nw.reset();
						nw.writeObject(vm);
						nw.close();
						sendSock.close();
						}
						catch (IOException e) {
							// TODO Auto-generated catch block
							Log.e("Unable to send forceinsert message at " + Globals.myPort," for " +vm.key + " " + vm.to);
						}
	 			}
				else if (vm.type.equalsIgnoreCase("globaldump"))
				{
					Log.e("Inside Client", "Going to send GlobalDump message from" + "   " + Globals.myPort);
					String nexNode = Globals.Alivelist.get((Globals.Alivelist.lastIndexOf(Globals.myPort)+1)%Globals.Alivelist.size());
					Socket sendSock1 =  new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
											Integer.parseInt(nexNode));
					
					virtualMessage wm = vm;
					synchronized(Globals.MessagesList)
					{
					for (Map.Entry<String, ArrayList<String>> entry : Globals.MessagesList.entrySet())
					{
						String key = entry.getKey();
					    ArrayList<String> value = entry.getValue();
					    //Log.v(" Global dump message received at " + Globals.myPort, key + " " + value.get(0));
						wm.messDump.put(key,value.get(0));
					}
					}
					try 
					{
						wm.GlobalmessDumpNodelist.add(Globals.myPort);
						wm.from = Globals.myPort;
						wm.to = nexNode;
						Log.e("Global dump called on me   ", wm.from + " Sending my lDump to  " + wm.to + " " + wm.type);
						ObjectOutputStream nw = new ObjectOutputStream(sendSock1.getOutputStream());
						nw.reset();
						nw.writeObject(wm);
						nw.flush();
						nw.close();
						sendSock1.close();
					}
					catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						nexNode = Globals.Alivelist.get((Globals.Alivelist.lastIndexOf(Globals.myPort)+2)%Globals.Alivelist.size());
						Socket sendSock2 =  new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
												Integer.parseInt(nexNode));
	  					//wm.GlobalmessDumpNodelist.add(Globals.myPort);
						wm.from = Globals.myPort;
						wm.to = nexNode;
						Log.e("Global dump called on me   ", wm.from + " Sending my lDump to  " + wm.to + " " + wm.type);
						ObjectOutputStream nw = new ObjectOutputStream(sendSock2.getOutputStream());
						nw.reset();
						nw.writeObject(wm);
						nw.flush();
						nw.close();
						sendSock2.close();
					}
				}
				else if (vm.type.equalsIgnoreCase("specific"))
				{
					Log.e("Inside Client", "Going to send Specific selection message from " + "   " + Globals.myPort);
					String nexNode = Globals.Alivelist.get((Globals.Alivelist.lastIndexOf(Globals.myPort)+1)%Globals.Alivelist.size());
					Socket sendSock =  new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
											Integer.parseInt(nexNode));
					
					virtualMessage wm = vm;
					//String sQuerValu = Globals.MessagesList.get(wm.key);
					ArrayList<String> sQuerValu = new ArrayList<String>(); 
					synchronized(Globals.MessagesList)
					{
		    		sQuerValu =	Globals.MessagesList.get(wm.key);
					}
					if ((sQuerValu != null))
		    		{
						wm.messDump.put(wm.key,sQuerValu.get(0));
						Log.v(" Specific selection found at " + Globals.myPort, " for key " + wm.key + " value " + sQuerValu.get(0));
		    		}
					try
					{
					wm.GlobalmessDumpNodelist.add(Globals.myPort);
					wm.from = Globals.myPort;
					wm.to = nexNode;
					Log.e("Specific key asked at  ", wm.from + " Sending it to  " + wm.to + " " + wm.type);
					ObjectOutputStream nw = new ObjectOutputStream(sendSock.getOutputStream());
					nw.reset();
					nw.writeObject(wm);
					nw.flush();
					nw.close();
					sendSock.close();
					}
					catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						
						nexNode = Globals.Alivelist.get((Globals.Alivelist.lastIndexOf(Globals.myPort)+2)%Globals.Alivelist.size());
						Socket sendSock2 =  new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
												Integer.parseInt(nexNode));
						
						//wm.GlobalmessDumpNodelist.add(Globals.myPort);
						wm.from = Globals.myPort;
						wm.to = nexNode;
						Log.e("Specific key sending failed  at   ", wm.from + " Sending it to  " + wm.to + " " + wm.type);
						Log.e("Specific key asked at   ", wm.from + " Sending it to  " + wm.to + " " + wm.type);
						ObjectOutputStream nw = new ObjectOutputStream(sendSock2.getOutputStream());
						nw.reset();
						nw.writeObject(wm);
						nw.flush();
						nw.close();
						sendSock2.close();
					}
				}
				else if (vm.type.equalsIgnoreCase("Recovery"))
				{
					
					if (vm.from==null && vm.to==null)
					{
						Log.v("Node at " + Globals.myPort,"Has crashed");
						virtualMessage mess = vm;
						int pos =0;
						for (int i =0; i< Globals.portStrHashlist.size();i++)
						{
							//Log.v(Globals.portStr, Globals.portStrHashlist.get(i) + " " +  Globals.portStrHashlist.size());
							if(Integer.parseInt(Globals.portStr) == Integer.parseInt(Globals.portStrHashlist.get(i)))
							{
								pos = i;
								break;
							}
							
						}
						Globals.prev2node = Globals.portStrHashlist.get(((((pos - 2)%Globals.portStrHashlist.size()) + Globals.portStrHashlist.size())%Globals.portStrHashlist.size()));
						Globals.prev2node = Integer.toString(Integer.parseInt(Globals.prev2node) * 2);
						Globals.prev1node = Globals.portStrHashlist.get(((((pos - 1)%Globals.portStrHashlist.size()) + Globals.portStrHashlist.size())%Globals.portStrHashlist.size()));
						Globals.prev1node = Integer.toString(Integer.parseInt(Globals.prev1node) * 2);
						Globals.succ1node = Globals.portStrHashlist.get(((((pos + 1)%Globals.portStrHashlist.size()) + Globals.portStrHashlist.size())%Globals.portStrHashlist.size()));
						Globals.succ1node = Integer.toString(Integer.parseInt(Globals.succ1node) * 2);
						Globals.succ2node = Globals.portStrHashlist.get(((((pos + 2)%Globals.portStrHashlist.size()) + Globals.portStrHashlist.size())%Globals.portStrHashlist.size()));
						Globals.succ2node = Integer.toString(Integer.parseInt(Globals.succ2node) * 2);
						Log.v("My previous succ and pred nodes at " + Globals.myPort,Globals.prev2node+Globals.prev1node+Globals.succ1node+Globals.succ2node);
						ArrayList<String> ptr = new ArrayList<String>();
						ptr.add(Globals.prev2node);
						ptr.add(Globals.prev1node);
						ptr.add(Globals.succ1node);
						ptr.add(Globals.succ2node);
						for (String S: ptr)
						{
							Log.v("Sending recovery message from " + Globals.myPort, "to " + S);
							mess.from = Globals.myPort;
							mess.to= S;
							
							if(S.equalsIgnoreCase(Globals.succ1node) || S.equalsIgnoreCase(Globals.succ2node))
							{
								//Log.v("Sending recovery message from " + Globals.myPort, " to successor " + S);
								mess.type = "RecoverySucc";
							}
							if(S.equalsIgnoreCase(Globals.prev2node) || S.equalsIgnoreCase(Globals.prev1node))
							{
								//Log.v("Sending recovery message from " + Globals.myPort, " to predecossor " + S);
								mess.type = "RecoveryPrev";
							}
							InsertClient ic = new InsertClient(mess);
							Thread t = new Thread(ic);
							t.start();
							t.join();
						}
						for (String S: ptr)
						{
							Log.v("Sending recovery message from " + Globals.myPort, "to " + S);
							mess.from = Globals.myPort;
							mess.to= S;
							
							if(S.equalsIgnoreCase(Globals.succ1node) || S.equalsIgnoreCase(Globals.succ2node))
							{
								//Log.v("Sending recovery message from " + Globals.myPort, " to successor " + S);
								mess.type = "RecoverySucc";
							}
							if(S.equalsIgnoreCase(Globals.prev2node) || S.equalsIgnoreCase(Globals.prev1node))
							{
								//Log.v("Sending recovery message from " + Globals.myPort, " to predecossor " + S);
								mess.type = "RecoveryPrev";
							}
							InsertClient ic = new InsertClient(mess);
							Thread t = new Thread(ic);
							t.start();
							t.join();
						}
						Globals.RecoveryLock = true;
					}
					else
					{
						InsertClient ic = new InsertClient(vm);
						Thread t = new Thread(ic);
						t.start();
						t.join();
						//Log.v("Recovery message received at "+ Globals.myPort,"and sent to " + vm.to);
					}
				}
				else if (vm.type.equalsIgnoreCase("inSucess"))
				{
					InsertClient ic = new InsertClient(vm);
					Thread t = new Thread(ic);
					t.start();
					//t.join();
					//Log.v("Insert success message client thread spawned at "+ Globals.myPort,"and sent to " + vm.to);
				}
				else if (vm.type.equalsIgnoreCase("delete"))
				{
					InsertClient ic = new InsertClient(vm);
					Thread t = new Thread(ic);
					t.start();
					t.join();
					//Log.v("Insert success message client thread spawned at "+ Globals.myPort,"and sent to " + vm.to);
				}
			}
			catch(Exception Ex)
			{
				Ex.printStackTrace();
			}  				
			return;
		}
	}
