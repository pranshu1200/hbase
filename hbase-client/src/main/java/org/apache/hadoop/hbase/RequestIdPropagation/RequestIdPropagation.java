package org.apache.hadoop.hbase.RequestIdPropagation;

import com.google.protobuf.Message;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RequestIdPropagation {
    final static Logger logger = LoggerFactory.getLogger(RequestIdPropagation.class);

    //id would be coming in from Phoenix-side(which will be combination of hostname,timestamp and an identifier(to distinguish a client making 2 requests at sane time),
    // here we are setting it to random) but in reality it will be set on Phoenix-side only to the combination described above
    // so if a user AB makes a request this Id will look like AB+timestamp+Identifier(say AB1234567)
    public static void assignInitialRequestId(Mutation mutation){
        Random rn=new Random();
        mutation.setId(Integer.toString(rn.nextInt()));
    }
    public static String extractRequestId(Mutation mutation){
        return mutation.getId();
    }
    public static List<String> extractRequestId(final ArrayList<Mutation>actions){
        List<String> requestIds=new ArrayList<>();
        for(int it=0;it<actions.size();it++){
            requestIds.add(extractRequestId(actions.get(it)));
        }
        return requestIds;
    }

    public static void propagateRequestId(Mutation mutation, ClientProtos.MutationProto.Builder builder){
        if(mutation.getId()!=null){
            builder.setMyTraceId(mutation.getId());
        }
    }
    public static void logRequestIdReached(Mutation mutation){
        System.out.println("request "+extractRequestId(mutation)+"reached in HBase");
        logger.info("request id {} reached in HBase",extractRequestId(mutation),mutation);
    }
    public static void logRequestIdReached(final ArrayList<Mutation>action){
        for(int it=0;it<action.size();it++){
            Mutation mutation=(Mutation)(action.get(it));
            logRequestIdReached(mutation);
        }
    }
    public static void logRequestIdReached(Map<ServerName, MultiAction<Row>> actionsByServer){
        for (Map.Entry<ServerName, MultiAction<Row>> e : actionsByServer.entrySet()) {
            ServerName server = e.getKey();
            MultiAction<Row> multiAction = e.getValue();
            for(Map.Entry<byte[],List<Action<Row>>> f : multiAction.actions.entrySet()){
                List<Action<Row>> actions=f.getValue();
                for(int ii=0;ii<actions.size();ii++){
                        Mutation m=(Mutation)(actions.get(ii).getAction());
                        if(m.getId()!=null) {
                            logger.debug("requestid {}. associated to server {}.", m.getId(), server.getServerName());
                        }
                        System.out.println("requestid "+m.getId()+"associated to server "+server.getServerName());
                }
            }
        }
    }

    public static void logRequestIdReached(Message param) {
        if (param.getClass().getName()
                == "org.apache.hadoop.hbase.protobuf.generated.ClientProtos$MultiRequest") {
            ClientProtos.MultiRequest var = (ClientProtos.MultiRequest) (param);
            for (int jj = 0; jj < var.getRegionActionList().size(); jj++) {
                for (int ii = 0; ii < var.getRegionActionList().get(jj).getActionList().size(); ii++) {
                    if (var.getRegionActionList().get(jj).getActionList().get(ii).getMutation().getMyTraceId()
                            !="") {
                        logger.info("mutation id {}. sent to RPCserver",
                                var.getRegionActionList().get(jj).getActionList().get(ii).getMutation().getMyTraceId());
                    }
                }
            }
        }
    }

    public static void requestReachedServer(Message msg){
        if(msg.getClass().getName()=="org.apache.hadoop.hbase.protobuf.generated.ClientProtos$MultiRequest") {
            ClientProtos.MultiRequest var= (ClientProtos.MultiRequest)(msg);
            for (int jj = 0; jj < var.getRegionActionList().size(); jj++) {
                for (int ii = 0; ii < var.getRegionActionList().get(jj).getActionList().size(); ii++) {
                    if(var.getRegionActionList().get(jj).getActionList().get(ii).getMutation().getMyTraceId()!="") {
                        logger.info("request id {}. received at server ",
                            var.getRegionActionList().get(jj).getActionList().get(ii).getMutation().getMyTraceId());
                    }
                }
            }
        }
        else if(msg.getClass().getName()=="org.apache.hadoop.hbase.protobuf.generated.ClientProtos$ScanRequest"){
            ClientProtos.ScanRequest var=(ClientProtos.ScanRequest)(msg);
            if(var.getScan().getMyRequestId()!="") {
                logger.info("server receives scan request {}", var.getScan().getMyRequestId());
            }
        }
    }
}
