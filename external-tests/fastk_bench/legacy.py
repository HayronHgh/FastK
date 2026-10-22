"""Historical JSON delivery, excluded from the five-backend native ranking."""

from operator import itemgetter

import numpy as np
from fastk_client import Client

from .batches import columns_for, dtype


class LegacyJSON:
    def __init__(self, root, binary):
        self.client = Client(root, binary=binary).__enter__()

    def query(self, query):
        if "feature" in query:
            result = self.client.read_scalar_range(
                query["symbol"],
                "1m",
                "feature",
                query["feature"],
                query["start_ts"],
                query["end_ts"],
            )
        else:
            result = self.client.read_kline_range(
                query["symbol"], "1m", query["start_ts"], query["end_ts"]
            )
        fields = columns_for(query)
        records = result["records"]
        batch = np.fromiter(
            map(itemgetter(*fields), records), dtype=dtype(fields), count=len(records)
        )
        batch.flags.writeable = False
        return batch

    def close(self):
        self.client.close()
