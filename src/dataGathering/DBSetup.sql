USE cse587;

DROP TABLE IF EXISTS nytPoliticsDocs;
DROP TABLE IF EXISTS nytSportsDocs;
DROP TABLE IF EXISTS nytBusinessDocs;
DROP TABLE IF EXISTS nytScienceDocs;

CREATE TABLE nytPoliticsDocs(
docID VARCHAR(24) PRIMARY KEY NOT NULL,
docURL text,
docDate DATE
);

CREATE TABLE nytSportsDocs(
docID VARCHAR(24) PRIMARY KEY NOT NULL,
docURL text,
docDate DATE
);

CREATE TABLE nytBusinessDocs(
docID VARCHAR(24) PRIMARY KEY NOT NULL,
docURL text,
docDate DATE
);

CREATE TABLE nytScienceDocs(
docID VARCHAR(24) PRIMARY KEY NOT NULL,
docURL text,
docDate DATE
);
