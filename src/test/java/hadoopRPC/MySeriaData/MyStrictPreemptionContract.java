package hadoopRPC.MySeriaData;

import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.util.Records;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyStrictPreemptionContract implements Serializable {
    public static MyStrictPreemptionContract newInstance(Set<PreemptionContainer> containers) {
        MyStrictPreemptionContract contract = new MyStrictPreemptionContract();

        contract.setContainers(containers);
        return contract;
    }
    private SetOfPreemptionContainer setOfPreemptionContainer;
    private class SetOfPreemptionContainer {
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

    public  Set<PreemptionContainer> getContainers(){
        return this.setOfPreemptionContainer.getSetOfPre();
    }


    public  void setContainers(Set<PreemptionContainer> containers){
        this.setOfPreemptionContainer = new SetOfPreemptionContainer(containers);
    }
    public StrictPreemptionContract transBack(){
        StrictPreemptionContract temp = StrictPreemptionContract.newInstance(
                this.setOfPreemptionContainer.getSetOfPre()
        );
        return temp;
    }
}
