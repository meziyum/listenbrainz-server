from flask import current_app
from psycopg2.extras import DictCursor, execute_values
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from listenbrainz.db import timescale as ts


def create_length_cache_tmp():
    mb_engine = create_engine(current_app.config["MBID_MAPPING_DATABASE_URI"], poolclass=NullPool)

    with mb_engine.connect() as mb_conn, \
            ts.engine.raw_connection() as lb_conn, \
            lb_conn.cursor(cursor_factory=DictCursor) as lb_curs:
        lb_curs.execute("CREATE TABLE mapping.recording_length_cache_tmp (gid UUID NOT NULL, length INT NOT NULL)")
        mb_curs = mb_conn.execute(text("SELECT gid, length FROM musicbrainz.recording WHERE length IS NOT NULL"))
        count = mb_curs.rowcount

        print("Total rows: ", count)
        done = 0
        while True:
            rows = mb_curs.fetchmany(1000)
            if not rows:
                break

            values = [(row["gid"], row["length"]) for row in rows]
            execute_values(lb_curs, "INSERT INTO mapping.recording_length_cache_tmp VALUES %s", values, page_size=1000)

            done += 1000
            print("Rows done: ", done)

        lb_curs.execute(
            "CREATE INDEX mapping.recording_length_cache_tmp_idx ON mapping.recording_length_cache_tmp(gid) INCLUDE (length)")
        lb_conn.commit()
