import psycopg2
from flask import current_app
from psycopg2.extras import DictCursor, execute_values

from listenbrainz.db import timescale as ts


def create_length_cache_tmp():
    with psycopg2.connect(current_app.config["MBID_MAPPING_DATABASE_URI"]) as mb_conn, \
            mb_conn.cursor('recording_length_cursor', cursor_factory=DictCursor) as mb_curs, \
            ts.engine.raw_connection() as lb_conn, \
            lb_conn.cursor(cursor_factory=DictCursor) as lb_curs:
        lb_curs.execute("CREATE TABLE mapping.recording_length_cache_tmp (gid UUID NOT NULL, length INT NOT NULL)")
        mb_curs.execute("SELECT gid, length FROM musicbrainz.recording WHERE length IS NOT NULL")

        print("Total rows: ", mb_curs.rowcount)
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
