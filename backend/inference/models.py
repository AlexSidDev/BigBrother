from abc import ABC, abstractmethod
import re
import torch
from transformers import PreTrainedTokenizer
from urlextract import URLExtract
import nltk
from backend.model_utils import preds_to_bio, to_device
from functools import partial


class BaseModel(ABC):
    def __init__(self, model, tokenizer: PreTrainedTokenizer, id_2_label: dict):
        self.model = model
        self.tokenizer = tokenizer
        self.max_len = 512
        self.id_2_label = {int(k): v for k, v in id_2_label.items()}
        self.extractor = URLExtract()

    def replace_url_username(self, raw_tweet: str, url_token, username_token):
        urls = self.extractor.find_urls(raw_tweet)
        for url in urls:
            raw_tweet = raw_tweet.replace(url, url_token)
        formatted_tweet = re.sub(r"\b(\s*)(@[\S]+)\b", username_token, raw_tweet)
        return formatted_tweet

    @abstractmethod
    def preprocess(self, raw_tweets: list):
        pass

    def inference(self, input_dict: dict):
        model_output = self.model.run(None, input_dict)[0].argmax(-1).tolist()
        return {'labels': model_output}

    @abstractmethod
    def postprocess(self, model_output: dict):
        pass

    def __call__(self, raw_tweets: list):
        preprocessed_tweet = self.preprocess(raw_tweets)
        model_output = self.inference(preprocessed_tweet)
        processed_output = self.postprocess(model_output)
        return processed_output


class ClassificationModel(BaseModel):
    def __init__(self, model, tokenizer: PreTrainedTokenizer, id_2_label: dict):
        super().__init__(model, tokenizer, id_2_label)
        self.replace_fn = partial(self.replace_url_username, url_token='http', username_token='@user')

    def preprocess(self, raw_tweets: list):
        formatted_tweet = list(map(self.replace_fn, raw_tweets))
        tokenized_inputs = self.tokenizer(formatted_tweet,
                                          max_length=self.max_len,
                                          truncation=True,
                                          padding=True,
                                          add_special_tokens=True)
        input_dict = dict(tokenized_inputs)
        return input_dict

    def postprocess(self, model_output: dict):
        return [self.id_2_label[output] for output in model_output['labels']]


class NerModel(BaseModel):
    def __init__(self, model, tokenizer: PreTrainedTokenizer, id_2_label: dict):
        super().__init__(model, tokenizer, id_2_label)
        self.convert_to_bio = lambda args: preds_to_bio(*args, self.id_2_label)
        self.replace_fn = partial(self.replace_url_username, url_token='{{URL}}', username_token=r'\1{\2@}')

    def preprocess(self, raw_tweets: list):
        formatted_tweet = list(map(self.replace_fn, raw_tweets))
        tokens = list(map(lambda tweet: tweet.split(' '), formatted_tweet))
        tokenized_inputs = self.tokenizer(tokens,
                                          max_length=self.max_len,
                                          truncation=True,
                                          padding=True,
                                          add_special_tokens=True,
                                          is_split_into_words=True)
        word_ids = [tokenized_inputs.word_ids(ind) for ind in range(len(tokenized_inputs.input_ids))]
        input_dict = dict(tokenized_inputs)
        input_dict['word_ids'] = word_ids
        return input_dict

    def inference(self, input_dict: dict):
        word_ids = input_dict.pop('word_ids')
        output = super().inference(input_dict)
        output['word_ids'] = word_ids
        return output

    def postprocess(self, model_output: dict):
        bio_labels = list(map(self.convert_to_bio, zip(model_output['labels'], model_output['word_ids'])))
        return bio_labels
