from enum import Enum

import dgl.function as fn
from torch import nn


class PoolingType(Enum):
    MIN = 'min'
    MAX = 'max'
    MEAN = 'mean'
    SUM = 'sum'

    def __str__(self):
        return self.value


class PoolingConv(nn.Module):

    def __init__(self, pooling_type=None):
        super().__init__()
        self.pooling_type = pooling_type

        dgl_mapping = {
            PoolingType.MIN: fn.min,
            PoolingType.MAX: fn.max,
            PoolingType.MEAN: fn.mean,
            PoolingType.SUM: fn.sum,
        }
        self.pooling_fn = dgl_mapping[self.pooling_type]

    def forward(self, graph=None, etypes=None, in_node_types=None, out_node_types=None, feat_dict=None):
        with graph.local_scope():
            graph.ndata['h'] = feat_dict

            # message passing
            graph.multi_update_all({etype: (fn.copy_src('h', 'm'), self.pooling_fn('m', 'ft')) for etype in etypes},
                                   cross_reducer=str(self.pooling_type))

            feat = graph.ndata['h']
            rst = graph.ndata['ft']

            # simply add to avoid overwriting old values
            out_dict = self.combine(feat, out_node_types, rst)
            return out_dict

    def combine(self, feat, out_node_types, rst):
        out_dict = dict()
        for out_type in out_node_types:
            if out_type in feat and out_type in rst:
                out_dict[out_type] = feat[out_type] + rst[out_type]
        return out_dict
