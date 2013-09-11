package com.whois.WBNP.model.vertex;
import com.infinitegraph.*;
import org.json.simple.*;
import java.util.*;

public class Country extends com.infinitegraph.BaseVertex
{
	protected String name = null;

	public Country()
	{
	}


	public String getKey()
	{
		fetch();
		return name;
	}
	public String get_name()
	{
		fetch();
		return this.name;
	}

	public void set_name(String value)
	{
		markModified();
		this.name = value;
	}

	public void assign(String[] row,int offset)
	{
		markModified();
		this.name = row[0+offset];
	}

	@SuppressWarnings("unchecked")
	public JSONObject toJSON()
	{
		JSONObject object = new JSONObject();
		object.put("oid",this.getId());
		object.put("name",this.get_name());
		return object;
	}
}
