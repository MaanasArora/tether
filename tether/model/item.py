import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F


class ItemAutoencoder(torch.nn.Module):
    def __init__(self, input_dim, hidden_dim, input_size=100):
        super(ItemAutoencoder, self).__init__()

        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.input_size = input_size

        self.input_linear = nn.Linear(input_dim, 64)
        self.encoder_lstm = nn.LSTM(
            input_size=64,
            hidden_size=hidden_dim,
            num_layers=2,
            batch_first=True,
            dropout=0.1,
        )
        self.decoder_lstm = nn.LSTM(
            input_size=hidden_dim,
            hidden_size=64,
            num_layers=2,
            batch_first=True,
            dropout=0.1,
        )
        self.output_linear = nn.Linear(64, input_dim)

    def encoder(self, x):
        x = F.relu(self.input_linear(x))
        x, _ = self.encoder_lstm(x)
        x = x[:, -1, :]
        return x

    def decoder(self, x):
        x = x.unsqueeze(1).repeat(1, self.input_size, 1)
        x, _ = self.decoder_lstm(x)
        x = self.output_linear(x)
        return x

    def forward(self, x):
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)

        return decoded


def load_model(model: torch.nn.Module, filepath: str):
    model.load_state_dict(torch.load(filepath))
    model.eval()
    return model


def process_ascii(ascii_items: list[str], max_length=100):
    one_hot_ascii = np.zeros((len(ascii_items), max_length, 256), dtype=np.float32)
    for i, item in enumerate(ascii_items):
        for j, char in enumerate(item):
            if j >= max_length:
                break
            if ord(char) < 255:  # Ensure character is within ASCII range
                one_hot_ascii[i, j, ord(char) + 1] = 1.0
            else:
                one_hot_ascii[i, j, 0] = 1.0

    if max_length > one_hot_ascii.shape[1]:
        padding = np.eye(256, dtype=np.float32)[0:1, :].reshape(1, 1, 256)
        one_hot_ascii = np.concatenate((one_hot_ascii, padding), axis=1)

    return one_hot_ascii
