package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import android.util.Log;

public class InsertClient implements Runnable {
	virtualMessage wm = new virtualMessage();
	public InsertClient(virtualMessage mess) {
		// TODO Auto-generated constructor stub
		wm = mess;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			//Log.v("InsertClient spawned at " + Globals.myPort,"Message being sent " + wm.from + wm.to+wm.key+wm.value +wm.type);
			Socket sendSock =  new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(wm.to));
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
				Log.e(" Insert client failed to send message at " + Globals.myPort," to " + wm.to);
			}
			return;
	}
	

}
