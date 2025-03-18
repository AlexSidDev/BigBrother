from abc import ABC, abstractmethod
import re
import torch
from transformers import PreTrainedTokenizer
from urlextract import URLExtract
from backend.model_utils import preds_to_bio, to_device


class BaseModel(ABC):
    def __init__(self, model, tokenizer: PreTrainedTokenizer, id_2_label: dict, device: str):
        self.model = model.to(device)
        self.tokenizer = tokenizer
        self.max_len = model.config.max_position_embeddings
        self.id_2_label = id_2_label
        self.device = device
        self.extractor = URLExtract()

    def replace_url_username(self, raw_tweet: str, url_token, username_token):
        urls = self.extractor.find_urls(raw_tweet)
        for url in urls:
            raw_tweet = raw_tweet.replace(url, url_token)
        formatted_tweet = re.sub(r"\b(\s*)(@[\S]+)\b", username_token, raw_tweet)
        return formatted_tweet

    @abstractmethod
    def preprocess(self, raw_tweet: str):
        pass

    def inference(self, input_dict: dict):
        input_dict = to_device(input_dict, self.device)
        with torch.no_grad():
            model_output = self.model(**input_dict).logits.argmax(-1).squeeze().tolist()
        return {'labels': model_output}

    @abstractmethod
    def postprocess(self, model_output: dict):
        pass

    def __call__(self, raw_tweet: str):
        preprocessed_tweet = self.preprocess(raw_tweet)
        model_output = self.inference(preprocessed_tweet)
        processed_output = self.postprocess(model_output)
        return processed_output


class ClassificationModel(BaseModel):
    def __init__(self, model, tokenizer: PreTrainedTokenizer, id_2_label: dict, device: str):
        super().__init__(model, tokenizer, id_2_label, device)

    def preprocess(self, raw_tweet: str):
        formatted_tweet = self.replace_url_username(raw_tweet, 'http', '@user')
        tokenized_inputs = self.tokenizer(formatted_tweet,
                                          return_tensors='pt',
                                          max_length=self.max_len,
                                          truncation=True,
                                          add_special_tokens=True)
        input_dict = {'input_ids': tokenized_inputs['input_ids']}
        return input_dict

    def postprocess(self, model_output: dict):
        return self.id_2_label[model_output['labels']]


class NerModel(BaseModel):
    def __init__(self, model, tokenizer: PreTrainedTokenizer, id_2_label: dict, device: str):
        super().__init__(model, tokenizer, id_2_label, device)

    def preprocess(self, raw_tweet: str):
        formatted_tweet = self.replace_url_username(raw_tweet, '{{URL}}', r'\1{\2@}')
        tokens = formatted_tweet.split(' ')
        tokenized_inputs = self.tokenizer(tokens,
                                          return_tensors='pt',
                                          max_length=self.max_len,
                                          truncation=True,
                                          add_special_tokens=True,
                                          is_split_into_words=True)
        row_tokens, word_ids = tokenized_inputs['input_ids'], tokenized_inputs.word_ids()
        input_dict = {'input_ids': row_tokens, 'word_ids': word_ids}
        return input_dict

    def inference(self, input_dict: dict):
        word_ids = input_dict.pop('word_ids')
        output = super().inference(input_dict)
        output['word_ids'] = word_ids
        return output

    def postprocess(self, model_output: dict):
        label_ids = model_output['labels']
        bio_labels = preds_to_bio(label_ids, model_output['word_ids'], self.id_2_label)
        return bio_labels
