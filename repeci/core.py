from sqlalchemy import Column, Integer, String, Table, ForeignKey, create_engine
from sqlalchemy.exc import IntegrityError
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

class Paper(Base):
    __tablename__ = "papers"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    handle = Column(String)

    authors = relationship("Author",
                    secondary="papers2authors",
                    backref="papers")
    jel = relationship("JEL",
                    secondary="papers2jel",
                    backref="papers")

class Author(Base):
    __tablename__ = "authors"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    code = Column(String, unique=True)

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
                try:
                    self.s.begin_nested()
                    author = Author(name=v)
                except IntegrityError:
                    print("Duplicate author")
                    self.s.rollback()
                    author = self.s.query(Author).filter(Author.name==v).first()
                else:
                    self.s.commit()

                paper.authors.append()
            elif k == "Classification-JEL":
                for c in v.split(", "):
                    paper.jel.append(JEL(code=c))
            elif k == "Handle":
                paper.handle = v
                self.s.add(paper)
        self.s.commit()

    def import_all(self):
        rfs = RFS(REPEC_ROOT_DIR)
        for file in rfs.realpaths():
            self.import_rdf(file)

def main():
    pass


if __name__ == '__main__':
    main()
