import torch
from tqdm import tqdm
import big_brother.core


def inference(model, val_dataloader, word_inds, labels_mapping, device='cuda'):
    preds = []
    for it, inputs in tqdm(enumerate(val_dataloader), total=len(val_dataloader)):
        with torch.no_grad():
            inputs = big_brother.core.to_device(inputs, device)
            inputs.pop('labels')

            outputs = model(**inputs).logits.argmax(dim=-1).cpu().numpy().tolist()
            preds.extend(outputs)

    bio_preds = []
    for i, row in enumerate(preds):
        bio_preds.append(big_brother.core.preds_to_bio(row, word_inds[i], labels_mapping))

    return bio_preds


