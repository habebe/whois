package com.whois.WBNP.model.edge;
import com.infinitegraph.*;
import org.json.simple.*;
import java.util.*;

public class IpDomain extends com.infinitegraph.BaseEdge
{
	protected double volume = 0.0;

	public IpDomain()
	{
	}


	public String getKey()
	{
		return null;
	}
	public double get_volume()
	{
		fetch();
		return this.volume;
	}

	public void set_volume(double value)
	{
		markModified();
		this.volume = value;
	}

	public void assign(String[] row,int offset)
	{
		markModified();
		this.volume = this.volume + Double.parseDouble(row[0+offset]);
	}

	@SuppressWarnings("unchecked")
	public JSONObject toJSON()
	{
		JSONObject object = new JSONObject();
		object.put("oid",this.getId());
		object.put("volume",this.get_volume());
		return object;
	}
}
