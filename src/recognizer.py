from itertools import groupby
from typing import Any
from .reader import read
from .pocketbase import PBManager
from .fingerprint import Fingerprint
from models import (
    Dataset, 
    FingerprintData, 
    RecordData, 
    Config, 
    MatchResult
)
import json

import os
cfg_path = os.path.join(os.path.dirname(__file__), 'config.json')
with open(cfg_path) as cfg:
    default_cfg: Config = Config(**json.load(cfg))

class Recognizer:
    
    def __init__(self, pbman: PBManager):
        self._pbman = pbman


    def upload_data(self, data: list[Dataset]):

        for elem in data:
            path = elem.path
            record_id = elem.record_id
            y, _ = read(path)
            hashes = Fingerprint(y)
            seen: set[tuple[str, int]] = set()
            for hash, offset in hashes:
                if (hash, offset) in seen:
                    continue
                fingerprint_data: FingerprintData = FingerprintData(**{
                    'hash': hash,
                    'record_id': record_id,
                    'offset': offset
                })
                self._pbman.upload_fingerprint(fingerprint_data)
                seen.add((hash, offset))

            record_data: RecordData = RecordData(**{
                'record_id': record_id,
                'fingerprinted': True
            })
            self._pbman.upload_record(record_data)


    def _align_offsets(
        self, 
        matches: list[tuple[str, int]], 
        dedup_hashes: dict[str, int],
        queried_hashes: int,
        top_n: int = default_cfg.top_n
    ) -> list[MatchResult]:
        
        sorted_matches = sorted(
            matches, 
            key=lambda m: (m[0], m[1])
        )
        counts = [
            (*key, len(list(group))) 
            for key, group in groupby(
                sorted_matches, 
                key=lambda m: (m[0], m[1])
            )
        ]
        record_matches = sorted(
            [
                max(list(group), key=lambda g: g[2]) 
                for _, group in groupby(
                    counts, 
                    key=lambda count: count[0]
                )
            ],
            key=lambda count: count[2], 
            reverse=True
        )
        record_results = []
        for record_id, offset, _ in record_matches[:top_n]:
            hashes_matched = dedup_hashes[record_id]
            result: MatchResult = {
                'record_id': record_id,
                'offset': offset,
                'offset_sec': offset / default_cfg.sample_rate * default_cfg.n_overlap,
                'confidence': hashes_matched / queried_hashes
            }
            record_results.append(result)
        return record_results


    def recognize_file(
        self, 
        filename: str, 
        start: int | None = None, 
        end: int | None = None
    ) -> list[MatchResult]:
        
        y, _ = read(filename)
        if start:
            start *= default_cfg.sample_rate
        if end:
            end *= default_cfg.sample_rate
        hashes = set(Fingerprint(y[start:end]))
        matches, dedup_hashes = self._pbman.match_fingerprints(hashes)
        results = self._align_offsets(matches, dedup_hashes, len(hashes))
        return results