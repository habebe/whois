package com.whois.WBNP.model.vertex;
import com.infinitegraph.*;
import org.json.simple.*;
import java.util.*;

public class Ip extends com.infinitegraph.BaseVertex
{
	protected String ip = null;

	public Ip()
	{
	}


	public String getKey()
	{
		fetch();
		return ip;
	}
	public String get_ip()
	{
		fetch();
		return this.ip;
	}

	public void set_ip(String value)
	{
		markModified();
		this.ip = value;
	}

	public void assign(String[] row,int offset)
	{
		markModified();
		this.ip = row[0+offset];
	}

	@SuppressWarnings("unchecked")
	public JSONObject toJSON()
	{
		JSONObject object = new JSONObject();
		object.put("oid",this.getId());
		object.put("ip",this.get_ip());
		return object;
	}
}
