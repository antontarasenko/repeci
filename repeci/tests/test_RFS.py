from unittest import TestCase

from repeci import RFS
from repeci.config import *


__author__ = "Anton Tarasenko <antontarasenko@gmail.com>"


class TestRFS(TestCase):
    def test_realpaths(self):
        rfs = RFS.RFS(REPEC_ROOT_DIR)
        self.assertGreater(rfs.realpaths(), 0)
