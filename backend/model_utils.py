from transformers import AutoModelForTokenClassification, BertForTokenClassification
import torch.nn as nn


def to_device(batch: dict, device: str):
    return {k: v.to(device) for k, v in batch.items()}


def preds_to_bio(preds: list, word_inds: list, labels_mapping: dict):
    bio_preds = []
    previous_ind = None
    for i, pred in enumerate(preds):
        if word_inds[i] is None or word_inds[i] == previous_ind:
            continue
        bio_preds.append(labels_mapping[pred])
        previous_ind = word_inds[i]
    return bio_preds


class ModelWithEmbeds(nn.Module):
    def __init__(self, model: BertForTokenClassification, tokens: int):
        super().__init__()
        self.model = model
        embed_dim = self.model.config.hidden_size
        self.custom_embeds = nn.Embedding(tokens, embed_dim)

    def forward(self, input_ids, attention_mask, extra_labels):
        extra_embeds = self.custom_embeds(extra_labels)
        input_embeds = self.model.bert.embeddings(input_ids)
        return self.model.forward(inputs_embeds=input_embeds + extra_embeds, attention_mask=attention_mask)

    def resize_token_embeddings(self, new_num_tokens: int):
        self.model.resize_token_embeddings(new_num_tokens)


def create_model(model_name_or_path: str, labels_mapping: dict, extra_tokens=None):
    model = AutoModelForTokenClassification.from_pretrained(model_name_or_path, num_labels=len(labels_mapping),
                                                            id2label={v: k for k, v in labels_mapping.items()},
                                                            label2id=labels_mapping)
    if extra_tokens is not None:
        model = ModelWithEmbeds(model, extra_tokens)
    return model
