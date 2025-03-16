import torch
import torch.nn as nn
from tqdm import tqdm
import os

import big_brother.core

class Trainer:
    def __init__(self, model,
                 optimizer,
                 train_dataloader,
                 val_dataloader,
                 device: str,
                 scheduler=None,
                 loss_weights=None):
        self.device = device
        self.model = model.to(device)
        self.train_dataloader = train_dataloader
        self.val_dataloader = val_dataloader

        if loss_weights is None:
            self.criterion = nn.CrossEntropyLoss(ignore_index=-100)
        else:
            self.criterion = nn.CrossEntropyLoss(loss_weights, ignore_index=-100)
        self.optimizer = optimizer
        self.scheduler = scheduler

    def train(self, epochs: int, save_path, val_every: int = 5, accumulation_step: int = 1):
        assert epochs % val_every == 0, 'Epochs number should be divisible by \'val_every\' parameter'
        assert accumulation_step > 0, '\'accumulation_step\' parameter should be greater than zero'
        os.makedirs(os.path.join('checkpoints', save_path), exist_ok=True)
        losses = []
        val_losses = []
        print('Start training')
        self.model.train()
        for epoch in range(epochs):
            mean_loss = 0
            mean_loss_val = 0
            self.model.train()
            for it, inputs in tqdm(enumerate(self.train_dataloader), total=len(self.train_dataloader), desc='Training'):
                inputs = big_brother.core.to_device(inputs, self.device)
                labels = inputs.pop('labels')

                logits = self.model(**inputs).logits
                num_targets = logits.shape[-1]
                loss = self.criterion(logits.view(-1, num_targets), labels.flatten()) / accumulation_step

                loss.backward()

                mean_loss += loss.item() / len(self.train_dataloader)
                if (it + 1) % accumulation_step == 0:
                    nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                    self.optimizer.step()
                    self.optimizer.zero_grad()

                if self.scheduler is not None:
                    self.scheduler.step()

            losses.append(mean_loss)
            print(f'Epoch: {epoch}, Mean loss: {mean_loss}')
            torch.save(self.model, os.path.join('checkpoints', save_path, 'best_model.pth'))

            if (epoch + 1) % val_every != 0:
                continue

            self.model.eval()
            for it, inputs in tqdm(enumerate(self.val_dataloader), total=len(self.val_dataloader),
                                   desc='Validation'):
                inputs = to_device(inputs, self.device)
                labels = inputs.pop('labels')
                with torch.no_grad():
                    logits = self.model(**inputs).logits
                num_targets = logits.shape[-1]
                loss = self.criterion(logits.view(-1, num_targets), labels.flatten())
                mean_loss_val += loss.item() / len(self.val_dataloader)
            val_losses.append(mean_loss_val)

            print(f'Mean Val loss: {mean_loss_val}')

        return losses, val_losses
