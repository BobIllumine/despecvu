from typing import Optional
from pydantic import BaseModel, Field


class Config(BaseModel):
    sample_rate: Optional[int] = Field(16000)
    n_fft: Optional[int] = Field(1024)
    n_overlap: Optional[int] = Field(64)
    n_perseg: Optional[int] = Field(128)
    fraction: Optional[float] = Field(0.02)
    mode: Optional[int] = Field(2)
    amp_min: Optional[float] = Field(1e-5)
    fan_value: Optional[int] = Field(15)
    mn_htd: Optional[int] = Field(0)
    mx_htd: Optional[int] = Field(200)
    peak_sort: Optional[bool] = Field(False)
    fingerprint_reduction: Optional[int] = Field(20)
    top_n: Optional[int] = Field(5)