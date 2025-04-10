import numpy as np
import pandas as pd


def extract_entities(tokens: list[str], labels: list[str], time: pd.Timestamp) -> pd.DataFrame:

    results = []
    entity = []
    category = None
    
    for token, label in zip(tokens, labels):
        if label.startswith('B-'):
            if entity:
                results.append({'entity': ' '.join(entity), 'category': category, 'time': time})
                entity = []
        
            category = label[2:]
            entity.append(token)
        
        elif label.startswith('I-') and entity:
            entity.append(token)
    
    if entity:
        results.append({'entity': ' '.join(entity), 'category': category, 'time': time})

    return results


def aggregate_entities(data: pd.DataFrame) -> pd.DataFrame:
    """
        Args:
            data (pd.DataFrame): dataset in the following format:
                    tokens	            time	    bio_labels
                0	[Morning, 5km...	2019-10-13	[O, O, ...
                1	[President,  ...	2019-11-03	[B-person, ...
                2	[", I, 've, ...	    2020-05-31	[O, O, ...

        Returns:
            pd.DataFrame: output in the following format:
                    entity	                        category	time
                0	pinkoctober	                    event	    2019-10-13
                1	breastcancerawareness	        event	    2019-10-13
                2	Central Park , Desa Parkcity	location	2019-10-13
    """
    entities = data.apply(lambda entry: extract_entities(tokens=entry.tokens, 
                                                         labels=entry.ner, 
                                                         time=entry.time),
                          axis=1)
    return pd.DataFrame([x for xs in entities for x in xs])


def calculate_expected_freq(freq_within: np.ndarray, 
                            freq_outside: np.ndarray, 
                            inverse_freq_within: np.ndarray, 
                            inverse_freq_outside: np.ndarray) -> np.ndarray:
    """
    Calculates expected frequency
    """
    total = freq_within + freq_outside + inverse_freq_within + inverse_freq_outside
    return (freq_within + freq_outside) * (freq_within + inverse_freq_within) / total


def calculate_chi_squared(observed_freq: np.ndarray, expected_freq: np.ndarray) -> np.ndarray:
    """
    Calculates chi quared values
    """
    return ((observed_freq - expected_freq) ** 2) / expected_freq
