import numpy as np
import torch
from torch import nn
from learning.constants import Feat
from learning.utils.embeddings import EmbeddingInitializer
from learning.utils.fc_out_model import FcOutModel


class NodeTypeEncoder(FcOutModel):
    """
    Model to encode one type of nodes in the graph (with particular features)
    """

    def __init__(self, features, feature_statistics, max_emb_dim=32, drop_whole_embeddings=False,
                 one_hot_embeddings=True, **kwargs):

        features.sort()

        for f in features:
            if f not in feature_statistics:
                raise ValueError(f"Did not find {f} in feature statistics")

        self.features = features
        self.feature_types = [feature_statistics[feat]['type']
                              for feat in features]
        self.feature_idxs = []

        # initialize embeddings and input dimension

        self.input_dim = 0
        self.input_feature_idx = 0
        embeddings = dict()
        for i, (feat, type) in enumerate(zip(self.features, self.feature_types)):
            if type == "numeric":
                # a single value is encoded here
                self.feature_idxs.append(
                    np.arange(self.input_feature_idx, self.input_feature_idx + 1))
                self.input_feature_idx += 1

                self.input_dim += 1
            elif type == "categorical":
                # similarly, a single value is encoded here
                self.feature_idxs.append(
                    np.arange(self.input_feature_idx, self.input_feature_idx + 1))
                self.input_feature_idx += 1

                embd = EmbeddingInitializer(feature_statistics[feat]['no_vals'], max_emb_dim, kwargs['p_dropout'],
                                            drop_whole_embeddings=drop_whole_embeddings, one_hot=one_hot_embeddings)
                embeddings[feat] = embd
                self.input_dim += embd.emb_dim
            else:
                raise NotImplementedError
        tmp = self.input_feature_idx
        super().__init__(input_dim=self.input_dim, **kwargs)

        self.embeddings = nn.ModuleDict(embeddings)

    def forward(self, input):
        assert input.shape[1] == self.input_feature_idx
        encoded_input = []
        for feat, feat_type, feat_idxs in zip(self.features, self.feature_types, self.feature_idxs):
            feat_data = input[:, feat_idxs]

            if feat_type == "numeric":
                encoded_input.append(feat_data)
            elif feat_type == "categorical":
                feat_data = torch.reshape(feat_data, (-1,))
                embd_data = self.embeddings[feat](feat_data.long())
                encoded_input.append(embd_data)
            else:
                raise NotImplementedError

        input_enc = torch.cat(encoded_input, dim=1)

        return self.fcout(input_enc)
