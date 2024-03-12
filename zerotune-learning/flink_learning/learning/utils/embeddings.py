import torch
from torch import nn


class EmbeddingInitializer(nn.Module):
    def __init__(self, num_embeddings, max_emb_dim, p_dropout, minimize_emb_dim=True, drop_whole_embeddings=False,
                 one_hot=False):
        """
        :param minimize_emb_dim:
            Whether to set embedding_dim = max_emb_dim or to make embedding_dim smaller is num_embeddings is small
        :param drop_whole_embeddings:
            If True, dropout pretends the embedding was a missing value. If false, dropout sets embed features to 0
        :param drop_whole_embeddings:
            If True, one-hot encode variables whose cardinality is < max_emb_dim. Also, set reqiures_grad = False
        """
        super().__init__()
        self.p_dropout = p_dropout
        self.drop_whole_embeddings = drop_whole_embeddings
        if minimize_emb_dim:
            self.emb_dim = min(max_emb_dim, num_embeddings) 
        else:
            self.emb_dim = max_emb_dim
        self.embed = nn.Embedding(num_embeddings=num_embeddings, embedding_dim=self.emb_dim)
        self.embed.weight.data.clamp_(-2, 2) 
        if one_hot:
            self.embed.weight.requires_grad = False
            if num_embeddings <= max_emb_dim:
                torch.eye(self.emb_dim, out=self.embed.weight.data)
        self.do = nn.Dropout(p=p_dropout)

    def forward(self, input):
        if self.drop_whole_embeddings and self.training:
            mask = torch.zeros_like(input).bernoulli_(1 - self.p_dropout)
            input = input * mask
        out = self.embed(input)
        if not self.drop_whole_embeddings:
            out = self.do(out)
        return out
