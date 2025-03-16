import pandas as pd
import torch
import torch.nn.functional as F


class NERDataset:
    def __init__(self, data: pd.DataFrame, extra_labels=False):
        self.data = data
        self.extra_labels = extra_labels

    def __len__(self):
        return len(self.data.index)

    def __getitem__(self, index):
        tokens = torch.tensor(self.data['tokens'][index], dtype=torch.long)
        labels = torch.tensor(self.data['labels'][index], dtype=torch.long)
        if not self.extra_labels:
            return tokens, labels
        return tokens, labels, torch.tensor(self.data['extra_labels'][index], dtype=torch.long)


class DataCollator:
    def __init__(self, token_pad_id: int, label_pad_id: int = -100):
        self.pad_id = token_pad_id
        self.label_pad_id = label_pad_id
        self.pad_fn = lambda sample, value, max_len: F.pad(sample, pad=(0, max_len - len(sample)), value=value)

    def __call__(self, data):
        batch = dict()
        max_len = max(list(map(lambda sample: sample[0].shape[-1], data)))
        padded_masks = [self.pad_fn(torch.ones_like(sample[0]), 0, max_len) for sample in data]
        batch['attention_mask'] = torch.stack(padded_masks)

        padded_ids = [self.pad_fn(sample[0], self.pad_id, max_len) for sample in data]
        batch['input_ids'] = torch.stack(padded_ids)

        padded_labels = [self.pad_fn(sample[1], self.label_pad_id, max_len) for sample in data]
        batch['labels'] = torch.stack(padded_labels)
        if len(data[0]) == 3:
            batch['extra_labels'] = torch.stack([self.pad_fn(sample[2], 0, max_len) for sample in data])
        return batch

