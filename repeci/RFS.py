from repeci.config import *

__author__ = "Anton Tarasenko <antontarasenko@gmail.com>"

import pandas as pd
import sqlite3 as sql
import os

class RFS():
    def __init__(self, root_dir):
        self.root_dir = root_dir
        self.files = pd.DataFrame(columns=['realpath', 'status'])
        for root, dirs, files in os.walk(root_dir):
            for file in files:
                if ".rdf" == file[-4:]:
                    self.files = self.files.append({'realpath': os.path.join(root, file),
                                         'status': 0},
                                        ignore_index=True)
        pass

    def realpaths(self):
        return self.files['realpath'].tolist()

def main():
    pass

if __name__ == '__main__':
    main()