package hadoopRPC.MySeriaData;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.util.Records;

import java.io.Serializable;
import java.util.*;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyPreemptionContract implements Serializable {

    public static MyPreemptionContract newInstance(
            List<PreemptionResourceRequest> req, Set<PreemptionContainer> containers) {
        MyPreemptionContract contract = new MyPreemptionContract();
        contract.setResourceRequest(req);
        contract.setContainers(containers);
        return contract;
    }

    private SetOfPreemptionContainer setOfPreemptionContainer;
    private PreemptionResourceRequestList preemptionResourceRequestList;

    public class SetOfPreemptionContainer{
        private Set<MyPreemptionContainer> myPreemptionContainersSet;

        public SetOfPreemptionContainer(Set<PreemptionContainer> containers) {

            myPreemptionContainersSet = new HashSet<MyPreemptionContainer>();
            for (Iterator it2 = containers.iterator(); it2.hasNext(); ) {
                PreemptionContainer temp = (PreemptionContainer) it2.next();
                myPreemptionContainersSet.add(
                        MyPreemptionContainer.newInstance(temp.getId())
                );

            }
        }

        public Set<PreemptionContainer> getSetOfPre() {
            Set<PreemptionContainer> containers = new HashSet<PreemptionContainer>();
            for (Iterator it2 = this.myPreemptionContainersSet.iterator(); it2.hasNext(); ) {
                PreemptionContainer temp = ((MyPreemptionContainer)it2.next()).transBack();
                containers.add(temp);
            }
            return containers;
        }
    }

    public class PreemptionResourceRequestList{
        private List<MyPreemptionResourceRequest> myPreemptionResourceRequestList;

        public PreemptionResourceRequestList(List<PreemptionResourceRequest> req) {
            this.myPreemptionResourceRequestList = new ArrayList<MyPreemptionResourceRequest>();
            for (Iterator it2 = req.iterator(); it2.hasNext(); ) {
                PreemptionResourceRequest temp = (PreemptionResourceRequest) it2.next();
                myPreemptionResourceRequestList.add(MyPreemptionResourceRequest.newInstance(
                        temp.getResourceRequest()
                ));
            }
        }

        public List<PreemptionResourceRequest> getPRRL() {
            List<PreemptionResourceRequest> req = new ArrayList<PreemptionResourceRequest>();
            for (Iterator it2 = this.myPreemptionResourceRequestList.iterator(); it2.hasNext(); ) {
                PreemptionResourceRequest temp = ((MyPreemptionResourceRequest) it2.next()).transBack();
                req.add(temp);
            }
            return req;
        }
    }

    public  List<PreemptionResourceRequest> getResourceRequest(){
            return this.preemptionResourceRequestList.getPRRL();
    }

    public  void setResourceRequest(List<PreemptionResourceRequest> req){
            this.preemptionResourceRequestList = new PreemptionResourceRequestList(req);
    }


    public  Set<PreemptionContainer> getContainers(){
            return this.setOfPreemptionContainer.getSetOfPre();
    }


    public  void setContainers(Set<PreemptionContainer> containers){
            this.setOfPreemptionContainer = new SetOfPreemptionContainer(containers);
    }

    public PreemptionContract transBack(){
        PreemptionContract temp = PreemptionContract.newInstance(
                this.getResourceRequest(),
                this.getContainers()
        );
        return temp;
    }
}
