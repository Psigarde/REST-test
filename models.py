from pydantic import BaseModel, validator, ValidationError
from typing import List, Optional
from enum import Enum

class DistributionType(str, Enum):
    uniform = "uniform"
    normal = "normal"
    weibull = "weibull"

class Sample(BaseModel):
    id: Optional[int]
    distributionType: DistributionType
    sampleCount : Optional[int]
    values : List[int]
    low: Optional[float]
    high: Optional[float]
    loc: Optional[float]
    scale: Optional[float]
    shape: Optional[float]