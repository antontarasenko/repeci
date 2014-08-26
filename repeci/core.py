from sqlalchemy import Column, Integer, String, Table, ForeignKey, create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, backref, joinedload
from repeci.RFS import RFS
from repeci.config import *

__author__ = "Anton Tarasenko <antontarasenko@gmail.com>"


Base = declarative_base()

papers2authors_table = Table('papers2authors', Base.metadata,
    Column('paper_id', Integer, ForeignKey('papers.id')),
    Column('author_id', Integer, ForeignKey('authors.id'))
)
# papers2jel_table = Table('papers2jel', Base.metadata,
#     Column('paper_id', Integer, ForeignKey('papers.id')),
#     Column('jel_id', Integer, ForeignKey('jel.id'))
# )

class Paper(Base):
    __tablename__ = "papers"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    handle = Column(String)

    authors = relationship("Author",
                    secondary="papers2authors",
                    backref="papers")
    # jel = relationship("JEL",
    #                 secondary="papers2jel",
    #                 backref="papers")
    def __repr__(self):
        return "<Paper %s>" % self.title

class Author(Base):
    __tablename__ = "authors"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    code = Column(String, unique=True)

    # papers = relationship("Paper",
    #                       secondary="papers2authors",
    #                       backref=backref("author", lazy='dynamic'))

    def __repr__(self):
        return "<Author %s %s %s>" % (self.id, self.name, self.code)

class JEL(Base):
    __tablename__ = "jel"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    code = Column(String, unique=True)

class DB():
    def __init__(self):
        engine = create_engine('sqlite:///' + REPECI_DB, echo=True)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        self.s = session

    def import_rdf(self, file):
        with open(file, 'r', encoding='latin-1') as f:
            lines = f.readlines()
        paper = Paper()
        for line in lines:
            br = line.find(':')
            k = line[:br]
            v = line[br+1:].strip()

            if k == "Title":
                paper.title = v
            elif k == "Year":
                paper.year = v
            elif k == "Author-Name":
                author = self.s.query(Author).filter(Author.name==v).first()
                if author is None:
                    author = Author(name=v)

                paper.authors.append(author)
            # elif k == "Classification-JEL":
            #     for c in v.split(", "):
            #         paper.jel.append(JEL(code=c))
            elif k == "Handle":
                paper.handle = v
                self.s.add(paper)
                self.s.commit()
                paper = Paper()


    def import_all(self, n):
        rfs = RFS(REPEC_ROOT_DIR)
        for i, file in enumerate(rfs.realpaths(), start=1):
            self.import_rdf(file)
            if i >= n: break

def main():
    db = DB()
    db.import_all(5)
    # db.s.query(Paper.title).filter(Paper.authors.any(Author.name.in_(["Anton"]))).all()
    db.s.close()
    pass

def one():
    print(1)

if __name__ == '__main__':
    main()
