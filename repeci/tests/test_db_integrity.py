from sqlalchemy import func
from repeci.core import DB, Author, Paper

__author__ = "Anton Tarasenko <antontarasenko@gmail.com>"

import unittest


class TestDBIntegrity(unittest.TestCase):
    def test_paper2author(self):
        db = DB()
        for c in db.s.query(func.count(Author.id)).group_by(Paper.id).all():
            self.assertTrue(0 < c < 5)


if __name__ == '__main__':
    unittest.main()
