__author__ = "Anton Tarasenko <antontarasenko@gmail.com>"

import networkx as nx


class RefGraph():
    def __init__(self, file=""):
        self.G = nx.Graph()
        if len(file) > 0:
            self.load_file(file)

    def load_file(self, file, n=0):
        lines = list()
        with open(file) as f:
            if n == 0:
                lines = f.readlines()
            else:
                c = 0
                while c < n:
                    lines.append(f.readline().strip())
                    c += 1
        print(len(lines), "lines read")
        edges = [(line.split(sep=" ")[0], line.split(sep=" ")[1]) for line in lines]
        self.G.add_edges_from(edges)
