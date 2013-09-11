/********************************************************************************

********************************************************************************/


package com.whois.WBNP.model;
import com.infinitegraph.*;
import com.infinitegraph.indexing.*;

public class QueryHandler
{
	public com.whois.WBNP.model.query.GraphIndex_Domain_name GraphIndex_Domain_name = new com.whois.WBNP.model.query.GraphIndex_Domain_name();
	public com.whois.WBNP.model.query.GraphIndex_Country_name GraphIndex_Country_name = new com.whois.WBNP.model.query.GraphIndex_Country_name();
	public com.whois.WBNP.model.query.GraphIndex_Registrar_name GraphIndex_Registrar_name = new com.whois.WBNP.model.query.GraphIndex_Registrar_name();
	public com.whois.WBNP.model.query.GraphIndex_Email_name GraphIndex_Email_name = new com.whois.WBNP.model.query.GraphIndex_Email_name();
	public com.whois.WBNP.model.query.GraphIndex_NameServer_name GraphIndex_NameServer_name = new com.whois.WBNP.model.query.GraphIndex_NameServer_name();
	public com.whois.WBNP.model.query.GraphIndex_Ip_ip GraphIndex_Ip_ip = new com.whois.WBNP.model.query.GraphIndex_Ip_ip();
	public java.util.HashMap<String,com.objectivity.ig.utility.KeylookupInterface> keylookupMap = new java.util.HashMap<String,com.objectivity.ig.utility.KeylookupInterface>();

	public QueryHandler()
	{
		keylookupMap.put("Domain",GraphIndex_Domain_name);
		keylookupMap.put("Country",GraphIndex_Country_name);
		keylookupMap.put("Registrar",GraphIndex_Registrar_name);
		keylookupMap.put("Email",GraphIndex_Email_name);
		keylookupMap.put("NameServer",GraphIndex_NameServer_name);
		keylookupMap.put("Ip",GraphIndex_Ip_ip);
	}

	public void createIndex() throws com.infinitegraph.indexing.IndexException
	{
		GraphIndex_Domain_name.createIndex();
		GraphIndex_Country_name.createIndex();
		GraphIndex_Registrar_name.createIndex();
		GraphIndex_Email_name.createIndex();
		GraphIndex_NameServer_name.createIndex();
		GraphIndex_Ip_ip.createIndex();
	}


	public com.objectivity.ig.utility.KeylookupInterface getKeylookupInterface(String name)
	{
		return keylookupMap.get(name);
	}

}
