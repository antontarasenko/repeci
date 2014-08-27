from sqlalchemy import func
from repeci.core import DB, Author, Paper

__author__ = "Anton Tarasenko <antontarasenko@gmail.com>"

import unittest


class TestDBIntegrity(unittest.TestCase):
    def test_paper2author(self):
        db = DB()
        for row in db.s.query(func.count(Author.id)).join(Paper.authors).group_by(Paper.id).all():
            self.assertTrue(0 < row[0] < 10)
        for c in db.s.query(func.count(Author.id)).join(Author.papers).group_by(Author.id).all():
            self.assertTrue(0 < row[0] < 100) # though many authors have 300+ items


if __name__ == '__main__':
    unittest.main()
