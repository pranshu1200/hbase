package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.RequestIdPropagation.RequestIdPropagation;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestIdFlow {
  final static Logger logger = LoggerFactory.getLogger(RequestIdPropagation.class);
  public static void propagateRequestId(Action action, ConnectionManager.HConnectionImplementation conn){
    Mutation m=(Mutation)(action.getAction());
    if(m.getId()!=null) {
      String s=m.getId();
      conn.setRequestId(s);
    }
  }
  public static void propagateRequestId(ConnectionManager.HConnectionImplementation conn,Scan scan){
    if(conn.getRequestId()!=null) {
      scan.setId(conn.getRequestId());
    }
  }
  public static void propagateRequestId(Scan scan,ClientProtos.Scan.Builder builder){
    if(scan.getId()!=null){
      builder.setMyRequestId(scan.getId());
    }
  }
  public static void propagateRequestId(Scan scan, ConnectionManager.HConnectionImplementation conn){
    if(scan.getId()!=null){
      conn.setRequestId(scan.getId());
    }
  }
}
