package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDatabase.CursorFactory;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class MainDatabaseHelper extends SQLiteOpenHelper {

	public MainDatabaseHelper(Context context, String name,
			CursorFactory factory, int version) {
		super(context, name, factory, version);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void onCreate(SQLiteDatabase db) {
		// TODO Auto-generated method stub
		final String SQL_CREATE_MAIN = "CREATE TABLE " +
    		    "Message_Dynamo" +                       // Table's name
    		    "(" +                           // The columns in the table
    		    " key STRING PRIMARY KEY, " +
    		    " value STRING," +
    		    " origport STRING " +
    		    ")";
    	db.execSQL(SQL_CREATE_MAIN);
		
	}

	@Override
	public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		// TODO Auto-generated method stub
		Log.e("On upgrade called at ", Globals.myPort);
		db.execSQL("DROP TABLE IF EXISTS Mlog");
		onCreate(db);
	}

}
