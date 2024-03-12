import dgl
from torch import nn

from learning.utils.fc_out_model import FcOutModel
from learning.message_aggregators import message_aggregators
from learning.utils.node_type_encoder import NodeTypeEncoder
from learning.constants import Featurization
import torch as th


class MyPassDirection:
    def __init__(self, model_name, e_name=None, n_src=None, n_dest=None, allow_empty=False):
        self.etypes = set()
        self.in_types = set()
        self.out_types = set()
        self.model_name = model_name

        self.etypes = [(n_src, e_name, n_dest)]
        self.in_types = [n_src]
        self.out_types = [n_dest]
        if not allow_empty:
            assert len(
                self.etypes) > 0, f"No nodes in the graph qualify for e_name={e_name}, n_dest={n_dest}"


class PassDirection:
    def __init__(self, model_name, g, e_name=None, n_dest=None, allow_empty=False):
        self.etypes = set()
        self.in_types = set()
        self.out_types = set()
        self.model_name = model_name

        for curr_n_src, curr_e_name, curr_n_dest in g.canonical_etypes:
            if e_name is not None and curr_e_name != e_name:
                continue

            if n_dest is not None and curr_n_dest != n_dest:
                continue

            self.etypes.add((curr_n_src, curr_e_name, curr_n_dest))
            self.in_types.add(curr_n_src)
            self.out_types.add(curr_n_dest)

        self.etypes = list(self.etypes)
        self.in_types = list(self.in_types)
        self.out_types = list(self.out_types)
        if not allow_empty:
            assert len(
                self.etypes) > 0, f"No nodes in the graph qualify for e_name={e_name}, n_dest={n_dest}"


class ZeroShotMessagePassingModel(FcOutModel):
    def __init__(self, hidden_dim=None, final_mlp_kwargs=None, output_dim=1, tree_layer_name=None,
                 tree_layer_kwargs=None, test=False, skip_message_passing=False):
        super().__init__(output_dim=output_dim, input_dim=hidden_dim,
                         final_out_layer=True, **final_mlp_kwargs)

        self.test = test
        self.skip_message_passing = skip_message_passing
        self.hidden_dim = hidden_dim

        operator_on_physical_model = message_aggregators.__dict__[
            tree_layer_name](hidden_dim=self.hidden_dim, **tree_layer_kwargs)
        operator_model = message_aggregators.__dict__[tree_layer_name](
            hidden_dim=self.hidden_dim, **tree_layer_kwargs)
        physical_model = message_aggregators.__dict__[tree_layer_name](
            hidden_dim=self.hidden_dim, **tree_layer_kwargs)
        # use different models per edge type
        self.tree_models = nn.ModuleDict({"operator_on_physical_model": operator_on_physical_model,
                                         "operator_model": operator_model, "physical_between_physical_model": physical_model})

    def encode_node_types(self, g, features):
        """
        Initializes the hidden states based on the node type specific models.
        """
        raise NotImplementedError

    def forward(self, input):
        """
        Returns logits for output classes
        """
        graph = input
        graph.features = self.encode_node_types(graph, graph.features)
        out = self.message_passing(graph)
        return out

    def message_passing(self, g):
        """
        Runs the GNN component of the model and returns logits for output classes.
        """
        pass_directions = g.pd
        feat_dict = g.features
        graph_data = g.data

        combined_e_types = set()
        for pd in pass_directions:
            combined_e_types.update(pd.etypes)
        assert combined_e_types == set(g.canonical_etypes)

        for pd in pass_directions:
            assert len(pd.etypes) > 0
            out_dict = self.tree_models[pd.model_name](g, etypes=pd.etypes, in_node_types=pd.in_types,
                                                       out_node_types=pd.out_types, feat_dict=feat_dict)
            for out_type, hidden_out in out_dict.items():
                feat_dict[out_type] = hidden_out

        outs = []
        g.ndata["feat"] = feat_dict
        graphs = dgl.unbatch(g)
        for graph_data, graph in zip(graph_data, graphs):
            out = graph.ndata["feat"][graph_data["top_node"]]
            out = out[graph_data["top_node_index"]]
            outs.append(out)
        outs = th.stack(outs, dim=0)
        if not self.test:
            outs = self.fcout(outs)
        # outs = th.reshape(outs, (1, int(outs.shape[0])))[0]
        return outs


class ZeroShotModel(ZeroShotMessagePassingModel):
    def __init__(self, hidden_dim=None, node_type_kwargs=None, feature_statistics=None, no_joins=False, **kwargs):
        super().__init__(hidden_dim=hidden_dim, **kwargs)

        self.plan_featurization = Featurization(no_joins)
        # different models to encode operators
        node_type_kwargs.update(output_dim=hidden_dim)
        # FLINK FEATURE DEFINITION
        self.node_type_encoders = nn.ModuleDict({
            'PhysicalNode': NodeTypeEncoder(self.plan_featurization.PHYSICAL_NODE_FEATURES, feature_statistics,
                                            **node_type_kwargs),
            'SourceOperator': NodeTypeEncoder(self.plan_featurization.SOURCE_FEATURES, feature_statistics,
                                              **node_type_kwargs),
            'FilterOperator': NodeTypeEncoder(self.plan_featurization.FILTER_FEATURES, feature_statistics,
                                              **node_type_kwargs),
            'WindowedJoinOperator': NodeTypeEncoder(self.plan_featurization.WINDOWED_JOIN_FEATURES, feature_statistics,
                                                    **node_type_kwargs),
            'WindowedAggregateOperator': NodeTypeEncoder(self.plan_featurization.WINDOWED_AGGREGATION,
                                                         feature_statistics, **node_type_kwargs),
        })

    def encode_node_types(self, g, features):
        """
        Initializes the hidden states based on the node type specific models.
        """
        # initialize hidden state per node type
        hidden_dict = dict()
        for node_type, input_features in features.items():
            # encode all plans with same model
            node_type_m = self.node_type_encoders[node_type]
            hidden_dict[node_type] = node_type_m(input_features)
        return hidden_dict
