import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))


from streamparse import Topology
from spouts.customer_spout import CustomerSpout
from bolts.churn_predictor import ChurnPredictorBolt
from spouts.churn_data_spout import ChurnDataSpout
from bolts.churn_data_bolt import ChurnDataBolt
from spouts.data_customer_spout import DataCustomerSpout
from bolts.data_customer_bolt import DataCustomerBolt
from spouts.data_customer_spout_with_stats import DataCustomerSpoutWithStats
from bolts.data_customer_bolt_with_stats import DataCustomerBoltWithStats
from spouts.customer_search_spout import CustomerSearchSpout
from bolts.customer_search_bolt import CustomerSearchBolt

class ChurnPredictionTopology(Topology):
    customer_spout = CustomerSpout.spec()
    churn_predictor_bolt = ChurnPredictorBolt.spec(inputs=[customer_spout])
   
    churn_spout = ChurnDataSpout.spec()
    churn_bolt = ChurnDataBolt.spec(inputs=[churn_spout])

    data_customer_spout = DataCustomerSpout.spec()
    data_customer_bolt = DataCustomerBolt.spec(inputs=[data_customer_spout])

    data_customer_spout_with_stats = DataCustomerSpoutWithStats.spec()
    data_customer_bolt_with_stats = DataCustomerBoltWithStats.spec(inputs=[data_customer_spout_with_stats])

    customer_search_spout = CustomerSearchSpout.spec()
    customer_search_bolt = CustomerSearchBolt.spec(inputs=[customer_search_spout])





