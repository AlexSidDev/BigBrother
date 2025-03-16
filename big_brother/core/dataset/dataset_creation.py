import os
import pandas as pd
from transformers import PreTrainedTokenizer
from ast import literal_eval
from dataset import Dataset


class DatasetTokenizer:
    def __init__(self, tokenizer: PreTrainedTokenizer, max_len: int):
        self.tokenizer = tokenizer
        self.max_len = max_len

    def re_tokenize_row(self, tokens, labels):
        tokenized_inputs = self.tokenizer(tokens, truncation=True,
                                          is_split_into_words=True,
                                          add_special_tokens=True,
                                          max_length=self.max_len)

        row_tokens, word_inds = tokenized_inputs['input_ids'], tokenized_inputs.word_ids()

        row_labels = []
        for word_ind in word_inds:
            if word_ind is None:
                row_labels.append(-100)
            else:
                row_labels.append(labels[word_ind])

        return [row_tokens, row_labels, word_inds]

    def re_tokenize(self, data: Dataset):
        tokens = data['tokens']
        labels = data['tags']
        processed_rows = []
        for row in range(len(tokens)):
            row_tokens = tokens[row]
            row_labels = labels[row]
            processed_row = self.re_tokenize_row(row_tokens, row_labels)
            processed_rows.append(processed_row)
        return pd.DataFrame(processed_rows, columns=['tokens', 'labels', 'word_inds'], dtype='object')


def create_dataset(raw_data: Dataset, save_file_name, tokenizer: PreTrainedTokenizer, max_len: int,
                   force_recreate=False):
    if not os.path.exists(save_file_name) or force_recreate:
        print("Start data processing...")
        re_tokenizer = DatasetTokenizer(tokenizer, max_len)
        processed_data = re_tokenizer.re_tokenize(raw_data)
        processed_data.to_csv(save_file_name)
    else:
        print("Found cached data in", save_file_name)

    all_data = pd.read_csv(save_file_name, index_col=0).map(literal_eval)
    return all_data



