
import logging
from datetime import datetime

from sqlalchemy import text

import listenbrainz.db.user as db_user
from listenbrainz import db
from listenbrainz.db.lastfm_user import User
from listenbrainz.db.testing import DatabaseTestCase, TimescaleTestCase
from listenbrainz.tests.utils import generate_data
from listenbrainz.webserver import timescale_connection


class TestAPICompatUserClass(DatabaseTestCase, TimescaleTestCase):

    @classmethod
    def setUpClass(cls):
        DatabaseTestCase.setUpClass()
        TimescaleTestCase.setUpClass()

    def setUp(self):
        DatabaseTestCase.setUp(self)
        TimescaleTestCase.setUp(self)
        self.log = logging.getLogger(__name__)
        self.logstore = timescale_connection._ts

        # Create a user
        uid = db_user.create(self.conn, 1, "test_api_compat_user")
        self.assertIsNotNone(db_user.get(self.conn, uid))

        result = self.conn.execute(text("""
            SELECT *
              FROM "user"
             WHERE id = :id
        """), {
            "id": uid,
        })
        row = result.fetchone()
        self.user = User(row['id'], row['created'], row['musicbrainz_id'], row['auth_token'])

    def tearDown(self):
        DatabaseTestCase.tearDown(self)
        TimescaleTestCase.tearDown(self)

    def test_user_get_id(self):
        uid = User.get_id(self.conn, self.user.name)
        self.assertEqual(uid, self.user.id)

    def test_user_load_by_name(self):
        user = User.load_by_name(self.conn, self.user.name)
        self.assertTrue(isinstance(user, User))
        self.assertDictEqual(user.__dict__, self.user.__dict__)

    def test_user_load_by_id(self):
        user = User.load_by_id(self.conn, self.user.id)
        self.assertTrue(isinstance(user, User))
        self.assertDictEqual(user.__dict__, self.user.__dict__)

    def test_user_get_play_count(self):
        date = datetime(2015, 9, 3, 0, 0, 0)
        test_data = generate_data(self.conn, date, 5, self.user.name)
        self.assertEqual(len(test_data), 5)
        self.logstore.insert(self.ts_conn, test_data)
        count = User.get_play_count(self.conn, self.ts_conn, self.user.id, self.logstore)
        self.assertIsInstance(count, int)
