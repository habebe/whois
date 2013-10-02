package com.objectivity.ig.utility;

@Deprecated
public class QueryResultHandler extends com.objy.db.internal.QueryResultHandler
{
  private com.objy.pm.ooId found = null;

  public Object found(com.objy.db.app.ooFDObj fd)
  {
    if (found != null)
    {
      return fd.objectFrom(this.found);
    }
    return null;
  }

  void reset()
  {
    found = null;
  }

  @Override
  public boolean onQueryResult(long queryNumber, String QueryName,
      com.objy.pm.ooId obj)
  {
    found = obj;
    return false;
  }
}
