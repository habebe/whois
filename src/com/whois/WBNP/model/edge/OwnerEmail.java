package com.whois.WBNP.model.edge;
import com.infinitegraph.*;
import org.json.simple.*;
import java.util.*;

public class OwnerEmail extends com.infinitegraph.BaseEdge
{

	public OwnerEmail()
	{
	}


	public String getKey()
	{
		return null;
	}
	public void assign(String[] row,int offset)
	{
		markModified();
	}

	@SuppressWarnings("unchecked")
	public JSONObject toJSON()
	{
		JSONObject object = new JSONObject();
		object.put("oid",this.getId());
		return object;
	}
}
