import torch
from tqdm import tqdm
from big_brother.core import to_device, preds_to_bio


def inference(model, val_dataloader, word_inds, labels_mapping, device='cuda'):
    preds = []
    for it, inputs in tqdm(enumerate(val_dataloader), total=len(val_dataloader)):
        with torch.no_grad():
            inputs = to_device(inputs, device)
            inputs.pop('labels')

            outputs = model(**inputs).logits.argmax(dim=-1).cpu().numpy().tolist()
            preds.extend(outputs)

    bio_preds = []
    for i, row in enumerate(preds):
        bio_preds.append(preds_to_bio(row, word_inds[i], labels_mapping))

    return bio_preds


