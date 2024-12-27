from typing import List, Tuple

import numpy as np
import psycopg

from dataset_reader.base_reader import Query
from engine.base_client.distances import Distance
from engine.base_client.search import BaseSearcher
from engine.clients.myduck.parser import MyDuckConditionParser
from engine.clients.myduck.config import get_db_config


class MyDuckSearcher(BaseSearcher):
    conn_wrapper = None
    distance = None
    search_params = {}
    parser = MyDuckConditionParser()

    # declare a wrapper to warp the connection object and cursor object
    # close them automatically if the wrapper which is the class object is deleted
    # and the connection object and cursor object are not closed
    class Wrapper:
        def __init__(self, conn, cur):
            self.conn = conn
            self.cur = cur

        def __del__(self):
            if self.cur and not self.cur.closed:
                self.cur.close()
                self.cur = None
            if self.conn and not self.conn.closed:
                self.conn.close()
                self.conn = None

    @classmethod
    def init_client(cls, host, dataset_config, connection_params: dict, search_params: dict):
        try:
            conn = psycopg.connect(**get_db_config(host, connection_params))
            cur = conn.cursor()
            cls.conn_wrapper = cls.Wrapper(conn, cur)
        except Exception as e:
            print(e)
        cls.conn_wrapper.cur.execute("LOAD vss;")
        cls.conn_wrapper.cur.execute(f"SET hnsw_ef_search = {search_params['config']['hnsw_ef']}")
        if dataset_config.distance == Distance.COSINE:
            cls.query = f"SELECT id, array_cosine_distance(embedding, %s::FLOAT[{dataset_config.vector_size}]) AS _score FROM items ORDER BY _score LIMIT %s"
        elif dataset_config.distance == Distance.L2:
            cls.query = f"SELECT id, array_distance(embedding, %s::FLOAT[{dataset_config.vector_size}]) AS _score FROM items ORDER BY _score LIMIT %s"
        else:
            raise NotImplementedError(f"Unsupported distance metric {cls.distance}")

    @classmethod
    def search_one(cls, query: Query, top) -> List[Tuple[int, float]]:
        formatted_query_vector = "[" + ",".join([f"{x:.10f}" for x in query.vector]) + "]"
        cls.conn_wrapper.cur.execute(
            cls.query, (formatted_query_vector, top), binary=True, prepare=True
        )
        return cls.conn_wrapper.cur.fetchall()

    @classmethod
    def delete_client(cls):
        if cls.conn_wrapper:
            del cls.conn_wrapper
            cls.conn_wrapper = None
