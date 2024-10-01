from typing import Any, Literal, Optional
from pocketbase import PocketBase
from models import FingerprintData, RecordData
from pocketbase.utils import ClientResponseError

class PBManager:

    def __init__(self, url: str, email: str, passwd: str):
        self._client = PocketBase(url)
        self._email = email
        self._passwd = passwd
        self._admin = self._client.admins.auth_with_password(self._email, self._passwd)
        if not self._admin.is_valid:
            raise PermissionError("Wrong credentials!")


    def set_collections(
        self, 
        fingerprint_col: str,
        record_col: str
    ):
        try:
            self._collections = {
                'fingerprints': self._client.collection(fingerprint_col),
                'records': self._client.collection(record_col)
            }
        except Exception as exc:
            raise Exception(
                'Collection retrieval failed. Check the names again.'
            ) from exc
    

    def _upload(
        self,
        collection_name: Literal['fingerprints', 'records'],
        data: dict[str, Any],
    ):
        try:
            self._collections[collection_name].create(data)
        except ClientResponseError as exc:
            raise Exception(
                f'Data upload to `{collection_name}` failed. {exc.data}'
            ) from exc


    def upload_record(
        self,
        record_data: RecordData,
    ):
        self._upload(
            collection_name='records', 
            data=record_data.model_dump()
        )

    
    def upload_fingerprint(
        self,
        fingerprint_data: FingerprintData
    ):
        self._upload(
            collection_name='fingerprints', 
            data=fingerprint_data.model_dump()
        )


    def _filter(
        self,
        collection_name: Literal['fingerprints', 'records'],
        filter: str,
        batch_size: int = 1000,
    ) -> list:
        try:
            return self._collections[collection_name].get_full_list(
                batch=batch_size, 
                query_params={
                    'filter': filter
                }
            )
        except ClientResponseError as exc:
            raise Exception(
                f'Filter in `{collection_name}` failed. {exc.data}'
            ) from exc
    

    def match_fingerprints(
        self,
        hashes: list[tuple[str, int]],
        batch_size: int = 500,
    ) -> tuple[list[tuple[str, int]], dict[str, int]]:
        bins = {}
        for hash, offset in hashes:
            if hash in bins.keys():
                bins[hash].append(offset)
            else:
                bins[hash] = [offset]
        dedup_hashes = {}
        values = list(bins.keys())

        results = []
        for i in range(0, len(values), batch_size):
            query_results = []
            filter = '(' + '||'.join(
                [
                    f'hash="{values[j]}"' 
                    for j in range(i, min(i + batch_size, len(values)))
                ]
            ) + ')'
            query_results.extend(self._filter('fingerprints', filter))
            for query_result in query_results:
                if query_result.record_id not in dedup_hashes.keys():
                    dedup_hashes[query_result.record_id] = 1
                else:
                    dedup_hashes[query_result.record_id] += 1
                for sampled_offset in bins[query_result.hash]:
                    results.append(
                        (query_result.record_id, query_result.offset - sampled_offset)
                    )
        return results, dedup_hashes


    def delete_all(
        self,
        collection_name: Literal['fingerprints', 'records'],
    ):
        for elem in  self._collections[collection_name].get_full_list(batch=10000):
            self._collections[collection_name].delete(id=elem.id)