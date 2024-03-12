from torch import nn
import torch
import torch.nn.functional as F

from learning.utils import activations, losses


class DynamicLayer(nn.Module):
    def __init__(self, p_dropout=None, activation_class_name=None, activation_class_kwargs=None, norm_class_name=None,
                 norm_class_kwargs=None, inplace=False, **kwargs):
        super().__init__()
        # initialize base NN
        self.inplace = inplace
        self.p_dropout = p_dropout
        self.act_class = activations.__dict__[activation_class_name]
        self.act_class_kwargs = activation_class_kwargs
        self.norm_class = nn.__dict__[norm_class_name]
        self.norm_class_kwargs = norm_class_kwargs

    def get_act(self):
        return self.act_class(inplace=self.inplace, **self.act_class_kwargs)

    def get_norm(self, num_feats):
        return self.norm_class(num_feats, inplace=self.inplace, **self.norm_class_kwargs)


# Residual block
class ResidualBlock(DynamicLayer):
    def __init__(self, layer_in, layer_out, norm=False, activation=False, dropout=False, final_out_layer=False,
                 **kwargs):
        super().__init__(**kwargs)
        assert layer_out == layer_in
        hidden_dim = layer_in

        self.activation = activation
        self.layers = nn.ModuleList()

        self.layers.append(nn.Linear(hidden_dim, hidden_dim))
        if norm:
            self.layers.append(self.get_norm(hidden_dim))
        if activation:
            self.layers.append(self.get_act())

        # if dropout:
        #     self.layers.append(nn.Dropout(self.p_dropout))
        self.layers.append(nn.Linear(hidden_dim, hidden_dim))
        if norm:
            self.layers.append(self.get_norm(hidden_dim))
        if dropout and not final_out_layer:
            # this can never be run inplace since it raises an error
            self.layers.append(nn.Dropout(self.p_dropout, inplace=False))

        if activation:
            self.final_act_layer = self.get_act()

    def forward(self, x):
        residual = x
        out = x
        for l in self.layers:
            out = l(out)

        out += residual
        if self.activation:
            out = self.final_act_layer(out)

        return out


class FcLayer(DynamicLayer):
    def __init__(self, layer_in, layer_out, norm=False, activation=False, dropout=False, final_out_layer=False,
                 **kwargs):
        super().__init__(**kwargs)

        layers = [nn.Linear(layer_in, layer_out)]
        if activation:
            layers.append(self.get_act())
        if norm:
            layers.append(self.get_norm(layer_out))
        if dropout and not final_out_layer:
            # this can never be run inplace since it raises an error
            layers.append(nn.Dropout(self.p_dropout, inplace=False))

        self.layers = nn.Sequential(*layers)

    def forward(self, x):
        return self.layers(x)


class FcOutModel(DynamicLayer):
    """
    Model with a Fully Connected Layer on top
    """

    def __init__(self, output_dim=None, input_dim=None, n_layers=None, width_factor=None,
                 residual=True, loss_class_name=None, loss_class_kwargs=None, task=None, **kwargs):
        super().__init__(**kwargs)

        self.task = task
        self.output_dim = output_dim
        self.input_dim = input_dim

        if loss_class_name == "MSE":
            self.loss_fxn = losses.MSELoss(self, **loss_class_kwargs)
        elif loss_class_name == "QLoss":
            self.loss_fxn = losses.QLoss(self, **loss_class_kwargs)

        layer_dims = [self.input_dim] + [int(width_factor * self.input_dim)] * n_layers + [self.output_dim]

        layers = []
        for layer_in, layer_out in zip(layer_dims, layer_dims[1:]):
            if not residual or layer_in != layer_out:
                layers.append(FcLayer(layer_in, layer_out, **kwargs))
            else:
                layers.append(ResidualBlock(layer_in, layer_out, **kwargs))

        self.fcout = nn.Sequential(*layers)
