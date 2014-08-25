import os
from unittest import TestCase
from repeci import RFS
from repeci.config import *

__author__ = "Anton Tarasenko <antontarasenko@gmail.com>"


class TestRFS(TestCase):
    # def test_rdf_papers(self):
    # self.fail()

    """
    def test_rdf_papers_chunks(self):
        rfs = RFS.RFS(os.getcwd())
        c = 0
        for realpath in rfs.realpaths():
            chunks = rfs.rdf_papers(realpath)
            for chunk in chunks:
                text = '\n'.join(chunk)
                for field in ["Title", "Handle"]:
                    self.assertEqual(text.count("\n%s: " % field), 1)
                c += 1
        self.assertGreater(c, 0)
    """

    def test_parse_rdf(self):
        rfs = RFS.RFS(os.getcwd())
        for file in rfs.realpaths():
            papers = rfs.parse_rdf(file)
            for paper in papers:
                self.assertGreater(len(paper), 3)
                for key in MIN_FIELDS:
                    self.assertTrue(key in paper.keys())