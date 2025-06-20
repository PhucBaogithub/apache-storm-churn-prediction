import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from streamparse import Topology
from spouts.churn_data_spout import ChurnDataSpout
from bolts.churn_predictor import ChurnPredictorBolt

class SimpleChurnTopology(Topology):
    churn_spout = ChurnDataSpout.spec()
    churn_predictor_bolt = ChurnPredictorBolt.spec(inputs=[churn_spout]) 