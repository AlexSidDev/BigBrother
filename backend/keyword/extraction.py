import datetime
from enum import Enum
from typing import TypedDict

import pandas as pd
import numpy as np

from backend.keyword.utils import aggregate_entities, calculate_chi_squared, calculate_expected_freq


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
        self.entities = aggregate_entities(data)
        self.criterion = {0.05: 3.842, 
                          0.01: 6.635, 
                          0.001: 10.828}
        
    def get_category(self, entity) -> str:
        return self.entities[self.entities.entity == entity].category.mode()[0]
    
    def get_frequencies(self,
                        start: datetime.datetime,
                        end: datetime.datetime) -> tuple[np.ndarray, ...]:
        
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
                         alpha: float = 0.05) -> list[tuple[str, str]]:
        assert alpha in self.criterion.keys(), 'alpha value incorrect'
        
        entities, observed_freq_within, \
            freq_outside, inverse_freq_within, \
                inverse_freq_outside = self.get_frequencies(start=start, end=end)

        expected_freq = calculate_expected_freq(freq_within=observed_freq_within,
                                                freq_outside=freq_outside,
                                                inverse_freq_within=inverse_freq_within,
                                                inverse_freq_outside=inverse_freq_outside)
        
        chi_squared_values = calculate_chi_squared(observed_freq=observed_freq_within,
                                                   expected_freq=expected_freq)
        
        top_entities_positions = np.argsort(chi_squared_values)[::-1][:top]
        top_entities = entities[top_entities_positions]
        top_chi_scores = chi_squared_values[top_entities_positions]

        return [RelevantNer(entity=entity, 
                            score=float(chi_score), 
                            category=self.get_category(entity))
                for entity, chi_score in zip(top_entities, top_chi_scores)]
