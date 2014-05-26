package edu.buffalo.cse.cse486586.simpledynamo;

import edu.buffalo.cse.cse486586.simpledynamo.OnTestClickListener;
import edu.buffalo.cse.cse486586.simpledynamo.R;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.database.Cursor;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	private Uri mUri;
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
        findViewById(R.id.button1).setOnClickListener(
				new OnClickListener(){
					@Override
					public void onClick(View arg0) 
					{
						new Ldump().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,(String[])null);
						Log.v("Local Dump called","Local Dump called");
					}

				});
        findViewById(R.id.button2).setOnClickListener(
				new OnClickListener(){
					@Override
					public void onClick(View arg0) 
					{
						new Gdump().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,(String[])null);
						Log.v("button click","global Dump called");
					}

				});
        findViewById(R.id.button4).setOnClickListener(
				new OnClickListener(){
					@Override
					public void onClick(View arg0) 
					{
						new Delete().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,(String[])null);
						Log.v("button click","Local Dump called");
					}

				});
	}
/*	@Override
	public void onStart()
	{
		super.onStart();
		virtualMessage mess = new virtualMessage(Globals.myPort,null,null,null,"Recovery");
		Client temp = new Client(mess);
		new Thread(temp).start();
	}
*/
	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	public class Ldump extends AsyncTask<String, String, Void> 
    {	
	String key,value = null;
 		@Override
 		protected Void doInBackground(String... msgs) 
 		{
 			String key = "@";	
			Cursor resultCursor = getContentResolver().query(mUri, null,key, null, null);
			int count =0;
			resultCursor.moveToFirst();
				while (resultCursor.isAfterLast() == false) 
				{
				   key = resultCursor.getString(0);
				   value = resultCursor.getString(1);
					resultCursor.moveToNext();
					count++;
					Log.v("Local dump", key + " " + value + " " + count);
					publishProgress(key + " " + value + " " + count);
				}
 			return null;
 		}
 		protected void onProgressUpdate(String... strings )
 		{
 			TextView tv1 = (TextView) findViewById(R.id.textView1);
 			if ( strings == null)
 			{
 				tv1.append("In L dump empty");
 			}
 			else
 			{
 			tv1.append("L Dump values " + strings[0] + "\n ");
 			}
 		}
 	}
    public class Gdump extends AsyncTask<String, String, Void> 
    {	
  	   //static final String REMOTE_PORT0 = "11108";
  	   String key,value = null;
 		@Override
 		protected Void doInBackground(String... msgs) 
 		{
 			String key = "*";
						
			Cursor resultCursor = getContentResolver().query(mUri, null,key, null, null);
			resultCursor.moveToFirst();
			while (resultCursor.isAfterLast() == false) 
			{
				key = resultCursor.getString(0);
				value = resultCursor.getString(1);
				resultCursor.moveToNext();
				//Log.v("Local dump", temp);
				publishProgress(key + " " + value);
			}
 			return null;
 		}
 		protected void onProgressUpdate(String... strings ){
 			TextView tv1 = (TextView) findViewById(R.id.textView1);
 			if ( strings[0] == null)
 			{
 				tv1.append("In G dump empty");
 			}
 			else
 			{
 			tv1.append("In G dump " + strings[0] + "\n ");
 			}
  	}
 	}
  public class Delete extends AsyncTask<String, String, Void> 
    {	
  	   //static final String REMOTE_PORT0 = "11108";
  	   String key,value = null;
 		@Override
 		protected Void doInBackground(String... msgs) 
 		{
 			String key = "key41";
						
			int resultCursor = getContentResolver().delete(mUri, key,null);
				publishProgress(key + " deleted " + resultCursor + " row ");
 			return null;
 		}
 		protected void onProgressUpdate(String... strings ){
 			TextView tv1 = (TextView) findViewById(R.id.textView1);
 			if ( strings[0] == null)
 			{
 				tv1.append("In delete");
 			}
 			else
 			{
 			tv1.append("Delete " + strings[0] + "\n ");
 			}
  	}
 	}

}
