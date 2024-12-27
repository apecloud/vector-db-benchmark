from typing import List

import numpy as np
import pandas as pd
import pyarrow as pa
import io
import psycopg

from dataset_reader.base_reader import Record
from engine.base_client import IncompatibilityError
from engine.base_client.distances import Distance
from engine.base_client.upload import BaseUploader
from engine.clients.myduck.config import get_db_config


class MyDuckUploader(BaseUploader):
    DISTANCE_MAPPING = {
        Distance.L2: "l2sq",
        Distance.COSINE: "cosine",
    }
    conn = None
    cur = None
    upload_params = {}

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params):
        cls.conn = psycopg.connect(**get_db_config(host, connection_params))
        cls.conn.execute("LOAD vss;")
        cls.cur = cls.conn.cursor()
        cls.upload_params = upload_params

    @classmethod
    def upload_batch(cls, batch: List[Record]):
        ids, vectors = [], []
        for record in batch:
            ids.append(record.id)
            vectors.append(record.vector)

        vectors = np.array(vectors)

        data = [(i, embedding) for i, embedding in zip(ids, vectors)]

        # Convert data to a pandas DataFrame
        df = pd.DataFrame(data, columns=['id', 'embedding'])

        # Convert pandas DataFrame to Arrow Table
        table = pa.Table.from_pandas(df)

        # Write Arrow Table to a BytesIO stream
        output_stream = io.BytesIO()
        with pa.ipc.RecordBatchStreamWriter(output_stream, table.schema) as writer:
            writer.write_table(table)

        # Use the COPY command to insert data in Arrow format
        cursor = cls.cur
        with cursor.copy(f"COPY items (id, embedding) FROM STDIN (FORMAT arrow)") as copy:
            copy.write(output_stream.getvalue())

    @classmethod
    def post_upload(cls, distance):
        return {}
        try:
            hnsw_distance_type = cls.DISTANCE_MAPPING[distance]
        except KeyError:
            raise IncompatibilityError(f"Unsupported distance metric: {distance}")

        cls.conn.execute("SET hnsw_enable_experimental_persistence = true;")
        m = cls.upload_params['hnsw_config']['m']
        m0 = cls.upload_params['hnsw_config']['m0'] if 'm0' in cls.upload_params['hnsw_config'] else m * 2
        efc = cls.upload_params['hnsw_config']['ef_construct']
        efs = cls.upload_params['hnsw_config']['ef_search'] if 'ef_search' in cls.upload_params['hnsw_config'] else efc
        cls.conn.execute(
            f"""
                CREATE INDEX items_{hnsw_distance_type}_{m}_{efc}
                ON items
                USING HNSW (embedding)
                WITH (
                    metric='{hnsw_distance_type}',
                    ef_construction={efc},
                    ef_search={efs},
                    m={m},
                    m0={m0});
            """
        )

        return {}

    @classmethod
    def delete_client(cls):
        if cls.cur:
            cls.cur.close()
            cls.conn.close()
