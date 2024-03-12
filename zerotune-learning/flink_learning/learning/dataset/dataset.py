from torch.utils.data import Dataset


class GraphDataset(Dataset):
    def __init__(self, graphs):
        self.graphs = graphs

    def __len__(self):
        return len(self.graphs)

    def __getitem__(self, i: int):
        return self.graphs[i]
