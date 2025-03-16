import datetime
from enum import Enum
from typing import TypedDict

import pandas as pd
import numpy as np

import big_brother.core


class Category(Enum):
    EVENT = 'EVENT'
    LOCATION = 'LOCATION'
    PERSON = 'PERSON'
    CORPORATION = 'CORPORATION'
    PRODUCT = 'PRODUCT'
    CREATIVE_WORK = 'CREATIVE_WORK'
    GROUP = 'GROUP'

    
class RelevantNer(TypedDict):
    entity: str
    category: Category
    score: float


class KeywordExtractor:

    def __init__(self, data: pd.DataFrame) -> None:
        """
        Args:
            data (pd.DataFrame): dataset in the following format:
                    tokens	            date	    bio_labels
                0	[Morning, 5km...	2019-10-13	[O, O, ...
                1	[President,  ...	2019-11-03	[B-person, ...
                2	[", I, 've, ...	    2020-05-31	[O, O, ...

        """

        self.entities = big_brother.core.aggregate_entities(data)
        self.criterion = {0.05: 3.842, 
                          0.01: 6.635, 
                          0.001: 10.828}
        
    def get_category(self, entity) -> str:
        """
        Return most common NER label for the requested entity.

        Args:
            entity (str): Requested entity.

        Returns:
            category (str): Most common category for entity.
        """
        return self.entities[self.entities.entity == entity].category.mode()[0]
    
    def get_frequencies(self,
                        start: datetime.datetime,
                        end: datetime.datetime) -> tuple[np.ndarray, ...]:
        """
        Get frequencies within and outside subcorpus.

        Args:
            start (datetime.datetime): Starting point.
            end (datetime.datetime): Ending point.


        Returns:
            entities (np.ndarray): List of entities in order.
            observed_freq_within (np.ndarray): Observed frequency.
            freq_outside (np.ndarray): Frequency in the rest of the texts.
            inverse_freq_within (np.ndarray): Inverse observed frequency.
            inverse_freq_outside (np.ndarray): Inverse frequency in the rest of the texts.
        """
        
        within = (self.entities.date > start) & (self.entities.date < end)
        frequencies_withith = self.entities[within].entity.value_counts()
        entities = frequencies_withith.index.values
        observed_freq_within = frequencies_withith.values
        freq_global = self.entities.entity.value_counts().loc[entities].values
        freq_outside = freq_global - observed_freq_within
        inverse_freq_within = observed_freq_within.sum() - observed_freq_within
        inverse_freq_outside = freq_global.sum() - freq_outside

        return (entities, 
                observed_freq_within, 
                freq_outside, 
                inverse_freq_within, 
                inverse_freq_outside)


    def extract_relevant(self, 
                         start: datetime.datetime,
                         end: datetime.datetime,
                         top: int = 10,
                         alpha: float = 0.05) -> list[RelevantNer]:
        """
        Extract top-N relevant NERs with a given alpha value.

        Args:
            start (datetime.datetime): Starting point.
            end (datetime.datetime): Ending point.
            top (int): Number of requested relevant NER.
            alpha (float): Significance level.

        Returns:
            list[RelevantNer]: Requested number of relevant NERs.
        """
        assert alpha in self.criterion.keys(), 'alpha value incorrect'
        
        entities, observed_freq_within, \
            freq_outside, inverse_freq_within, \
                inverse_freq_outside = self.get_frequencies(start=start, end=end)

        expected_freq = big_brother.core.calculate_expected_freq(freq_within=observed_freq_within,
                                                                 freq_outside=freq_outside,
                                                                 inverse_freq_within=inverse_freq_within,
                                                                 inverse_freq_outside=inverse_freq_outside)
        
        chi_squared_values = big_brother.core.calculate_chi_squared(observed_freq=observed_freq_within,
                                                                    expected_freq=expected_freq)
        
        top_entities_positions = np.argsort(chi_squared_values)[::-1][:top]
        top_entities = entities[top_entities_positions]
        top_chi_scores = chi_squared_values[top_entities_positions]

        return [RelevantNer(entity=entity, 
                            score=float(chi_score), 
                            category=self.get_category(entity))
                for entity, chi_score in zip(top_entities, top_chi_scores)]
