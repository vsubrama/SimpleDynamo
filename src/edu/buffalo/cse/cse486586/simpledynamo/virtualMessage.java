package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;


public class virtualMessage implements Serializable{

	private static final long serialVersionUID = 1L;
	public String from;
	public String to;
	public String key;
	public String type;
	public String value;
	public String OriginalInsert;
	public LinkedHashMap<String,String> messDump = new LinkedHashMap<String,String>();
	public ArrayList<String> GlobalmessDumpNodelist = new ArrayList<String>();
	//
	//public Cursor reCursor;
	virtualMessage(String from,String to,String key, String value, String type, String OriginalInsert)
	{
		this.key = key;
		this.value = value;
		this.type = type;
		this.from = from;
		this.to = to;
		this.OriginalInsert = OriginalInsert;
	}
	virtualMessage()
	{
	
	}
}
