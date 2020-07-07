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
      conn.setRequestId(m.getId());
      logRequestIdReached(conn);
    }
  }
  public static void propagateRequestId(ConnectionManager.HConnectionImplementation conn,Scan scan){
    if(conn.getRequestId()!=null) {
      scan.setId(conn.getRequestId());
      logRequestIdReached(scan);
    }
  }
  public static void propagateRequestId(Scan scan,ClientProtos.Scan.Builder builder){
    if(scan.getId()!=null){
      builder.setMyRequestId(scan.getId());
      logRequestIdReached(builder);
    }
  }
  public static void propagateRequestId(Scan scan, ConnectionManager.HConnectionImplementation conn){
    if(scan.getId()!=null){
      conn.setRequestId(scan.getId());
      logRequestIdReached(conn);
    }
  }
  public static void logRequestIdReached(ClientProtos.Scan.Builder builder){
    logger.info("scan protobuff created for request {}.",builder.getMyRequestId());
  }
  public static void logRequestIdReached(Scan scan){
    if(scan.getId()!=null) {
      logger.debug("scanning meta table to get Region for Request {}.", scan.getId());
    }
  }
  public static void logRequestIdReached(ConnectionManager.HConnectionImplementation conn){
    if(conn.getRequestId()!=null) {
      logger.info("Assigned {}. to connection {}. ", conn.getRequestId(), conn);
    }
  }
  public static void logLookRegion(Action action){
    Mutation m=(Mutation)(action.getAction());
    if(m.getId()!=null) {
      logger.info("Looking for region for query {}.", m.getId());
    }
  }
  public static void logRegionFoundInCache(String s){
    if(s!=null) {
      logger.info("Found region for Request {}. in cache", s);
    }
  }

  public static  void logMetaFoundInCache(ConnectionManager.HConnectionImplementation conn){
    if(conn.getRequestId()!=null){
      logger.info("Found meta table in cache for request {}.",conn.getRequestId());
    }
  }

  public static  void logMetaFoundInZK(ConnectionManager.HConnectionImplementation conn){
    if(conn.getRequestId()!=null){
      logger.info("Found meta table in ZK for request {}.",conn.getRequestId());
    }
  }

}
