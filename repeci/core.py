import os
import itertools
import time

import numpy as np
import pandas as pd
import networkx as nx
from sqlalchemy import Column, Integer, String, Table, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

from repeci.RFS import RFS
from repeci.config import *


__author__ = "Anton Tarasenko <antontarasenko@gmail.com>"


Base = declarative_base()

papers2authors_table = Table('papers2authors', Base.metadata,
    Column('paper_id', Integer, ForeignKey('papers.id')),
    Column('author_id', Integer, ForeignKey('authors.id'))
)
papers2jel_table = Table('papers2jel', Base.metadata,
    Column('paper_id', Integer, ForeignKey('papers.id')),
    Column('jel_id', Integer, ForeignKey('jel.id'))
)
citing2cited_table = Table('citing2cited', Base.metadata,
                           Column('citing', Integer, ForeignKey('papers.id'), primary_key=True),
                          Column('cited', Integer, ForeignKey('papers.id'), primary_key=True)
)

class Paper(Base):
    __tablename__ = "papers"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    year = Column(Integer)
    handle = Column(String, unique=True)

    refs = relationship("Paper",
                        secondary="citing2cited",
                        primaryjoin="Paper.id==citing2cited.c.citing",
                        secondaryjoin="Paper.id==citing2cited.c.cited",
                        backref="cited_by")
    authors = relationship("Author",
                    secondary="papers2authors",
                    backref="papers")
    jel = relationship("JEL",
                    secondary="papers2jel",
                    backref="papers")
    def __repr__(self):
        return '<Paper %d "%s" %d %s>' % (self.id, self.title, self.year, self.handle)

class Author(Base):
    __tablename__ = "authors"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    code = Column(String, unique=True)

    def __repr__(self):
        return "<Author %s %s %s>" % (self.id, self.name, self.code)

class JEL(Base):
    __tablename__ = "jel"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    code = Column(String, unique=True)

    def __repr__(self):
        return "<JEL %s %s %s>" % (self.id, self.name, self.code)

class DB():
    def __init__(self):
        engine = create_engine('sqlite:///' + REPECI_DB, echo=False)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        self.s = session

    def url(self, handle):
        t = handle.split(":")
        url = "http://ideas.repec.org/p/" + "/".join(t[1:]) + ".html"
        return url

    def pd(self):
        df = pd.DataFrame(self.s.\
                          query(Paper.title, Paper.year, Author.name, JEL.code).\
            join(Paper.authors).\
            join(Paper.jel).\
            all(),
                          columns=['title', 'year', 'author', 'jel'])
        print("Exported to DataFrame")
        return df

    def ba_table(self):
        df = self.pd()
        tab = pd.DataFrame(columns=['k', 'i', 'j', 'B_ijk', 'A_ijk', 'C_ijk', 'N_ijk'])
        for k in set(df['title'].tolist()):
            year = df[df['title'] == k]['year'].tolist()[0]
            authors = set(df[df['title'] == k]['author'].tolist())
            for i, j in [itertools.permutations(authors, 2)]:
                B_ijk = set(df[df['title'] == k & df['year'] < year]['jel'].tolist())
                A_ijk = set(df[df['title'] == k & df['year'] > year]['jel'].tolist())
                C_ijk = B_ijk & A_ijk
                N_ijk = A_ijk - C_ijk

        for row in tab.iterrows():
            i, j = row[1]['i'], row[1]['j']
            ix = tab.i == i & tab.j == j
            tab[ix]['T_jik'] = tab[tab.j == i & tab.i == j]['B_ijk'] & tab['N_ijk']
            tab[ix]['r_jik'] = len(tab[ix]['T_jik']) / len(tab[ix]['A_ijk'])

        r_ji = tab['r_jik'].groupby(['i', 'j']).agg['mean']
        return r_ji

    def import_refs(self, file, n=0):
        lines = list()
        with open(file) as f:
            if n == 0:
                lines = f.read().splitlines()
            else:
                c = 0
                while c < n:
                    lines.append(f.readline().strip())
                    c += 1
        print(len(lines), "lines read")
        for line in lines:
            sep = line.split(sep=" ")
            cited = sep[0]
            cited_instance = self.s.query(Paper).filter(Paper.handle == cited).first()
            if cited_instance is None:
                cited_instance = Paper(handle=cited)
            for citing in set(sep[1].split(sep="#")):
                citing_instance = self.s.query(Paper).filter(Paper.handle == citing).first()
                if citing_instance is None:
                    citing_instance = Paper(handle=citing)
                if citing_instance.handle != cited_instance.handle:
                    cited_instance.cited_by.append(citing_instance)
            self.s.add(cited_instance)
            self.s.flush()
        self.s.commit()

    def ref_graph(self):
        s = time.perf_counter()
        # MultiDiGraph isn't supported by pagerank() and other algorithms
        G = nx.DiGraph()
        for (cited,) in self.s.query(Paper.handle).all():
            for (citing,) in self.s.query(Paper.handle).filter(Paper.refs.any(Paper.handle == cited)).all():
                G.add_edge(cited, citing)
        e = time.perf_counter()

        print("Graph building is completed in %d seconds" % round(e - s, 1))
        return G

    def ref_metrics(self, G):
        s = time.perf_counter()

        # See https://networkx.github.io/documentation/latest/reference/algorithms.html for algoriths
        # Some algorithms don't support directed graphs

        df = pd.DataFrame([nx.out_degree_centrality(G),
                           nx.pagerank(G),
                           nx.betweenness_centrality(G),
                           nx.closeness_centrality(G),
                           # nx.current_flow_betweenness_centrality(G),
                           # nx.current_flow_closeness_centrality(G),
                           # nx.eigenvector_centrality(G)
        ]).T
        df.columns = ['odc', 'pr', 'bc', 'cc',
                      # 'cfbc', 'cfcc', 'ec'
        ]

        e = time.perf_counter()
        print("Metrics is computed in %d seconds" % round(e - s, 1))
        return df

    def ref_pagerank_a(self, G):
        '''
        Check `nx.pagerank()` sensitivity for alpha.
        :param G: graph
        :return: pd.DataFrame of comparative statistics
        '''
        df = pd.DataFrame()
        for a in np.arange(.5, 1, .05):
            nxpr = nx.pagerank(G, alpha=a)
            df = df.join(pd.DataFrame(list(nxpr.values()),
                                      columns=[str(a)],
                                      index=nxpr.keys()),
                         how='outer')
        return df

    def import_rdf(self, file):
        with open(file, 'r', encoding='latin-1') as f:
            lines = f.readlines()
        paper = Paper()
        is_article = False
        for line in lines:
            br = line.find(':')
            k = line[:br]
            v = line[br+1:].strip()

            if k.lower() == "template-type":
                is_article = True if v == "ReDIF-Article 1.0" else False

            if is_article:
                if k == "Title":
                    paper.title = v
                elif k == "Year":
                    paper.year = v
                elif k == "Author-Name":
                    author = self.s.query(Author).filter(Author.name == v).first()
                    if author is None:
                        author = Author(name=v)
                    paper.authors.append(author)
                elif k == "Classification-JEL":
                    codes = {c.strip() for c in v.split(", ")}
                    for c in codes:
                        if len(c) != 3:
                            raise ValueError("Not a 3-letter code")
                        jel = self.s.query(JEL).filter(JEL.code == c).first()
                        if jel is None:
                            jel = JEL(code=c)
                        paper.jel.append(jel)
                elif k == "Handle":
                    paper.handle = v
                    # TODO is this check necessary?
                    # paper_exists = self.s.query(Paper).filter(Paper.handle==paper.handle).first()
                    # if paper_exists is not None:
                    # paper =
                    if len(paper.authors) == 0:
                        raise ImportError("An article with empty authors:", paper.handle)
                    self.s.add(paper)
                    self.s.commit()
                    print("Paper added:", paper.handle)
                    paper = Paper()


    def import_all(self, file, n=0):
        rfs = RFS(REPEC_OPT_DIR)
        if n == 0:
            for file in rfs.realpaths():
                self.import_rdf(file)
        else:
            for i, file in enumerate(rfs.realpaths(), start=1):
                self.import_rdf(file)
                if i >= n: break
        print("Importing is completed")


def main():
    if RECREATE_DB:
        try:
            os.remove(REPECI_DB)
            print("File removed:", REPECI_DB)
        except FileNotFoundError:
            if str(input("File not found. Continue with a new one? [y/n] ")) == "n":
                return None
        except OSError as e:
            print("Failed with:", e.strerror)
            print("Error code:", e.code)
    db = DB()

    s = time.perf_counter()
    db.import_all(REPEC_OPT_DIR, n=RDF_MAX)
    e = time.perf_counter()
    print("%d rdf files have been imported in %d seconds" % (RDF_MAX, round(e - s, 1)))

    s = time.perf_counter()
    db.import_refs(REPEC_REFS_FILE, n=REFS_MAX)
    e = time.perf_counter()
    print("%d reference nodes have been imported in %d seconds" % (REFS_MAX, round(e - s, 1)))

    db.s.close()

if __name__ == '__main__':
    main()
