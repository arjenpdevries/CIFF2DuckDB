import duckdb
import pyarrow as pa

from ciff_toolkit.read import CiffReader
from ciff_toolkit.ciff_pb2 import DocRecord, Header, PostingsList
from google.protobuf.json_format import MessageToJson, MessageToDict
from typing import Iterator, TypeVar, Iterable

pbopt = {"including_default_value_fields": True, 
         "preserving_proto_field_name": True}
#
# Generator for reading batches of postings
#
# Note: Term identifiers handed out here, while reading term-posting pairs from the CIFF file
def iter_posting_batches(reader: Iterable[PostingsList]):
    batch = []
    for tid, p in enumerate(reader.read_postings_lists()):
        prevdocid = 0
        pp = MessageToDict(p, **pbopt)
        pp['termid']=tid
        # Gap Decompression...
        pp['postings']=[prev := {"docid":0}] and \
            [prev := {"docid": posting['docid'] + prev['docid'], "tf": posting['tf']} for posting in pp['postings']]
        batch.append(pp)
        if len(batch) == 4096:
            yield pa.RecordBatch.from_pylist(batch)
            batch = []
    yield pa.RecordBatch.from_pylist(batch)

#
# Generator for reading batches of docs
def iter_docs_batches(reader: Iterable[DocRecord]):
    batch = []
    for doc in reader.read_documents():
        batch.append(MessageToDict(doc, **pbopt))
        if len(batch) == 8192:
            yield pa.RecordBatch.from_pylist(batch)
            batch = []
    yield pa.RecordBatch.from_pylist(batch)

#
# MAIN:
#

# 
# Schema: manually defined 
# (alternative: protarrow could create the datastructure from the proto definition)
postings_schema = pa.schema([
    ("term", pa.string()),
    ("termid", pa.int64()),
    ("df", pa.int64()),
    ("cf", pa.int64()),
    ("postings", pa.list_(pa.struct([
        ("docid", pa.int32()),
        ("tf", pa.int32())
        ])))
     ])

docs_schema = pa.schema([
    ("docid", pa.int32()),
    ("collection_docid", pa.string()),
    ("doclength", pa.int32())
     ])

#
# Create/open DuckDB database
con = duckdb.connect("./ciff.db")

#
# Use CIFFReader to create RecordBatches for table (using Arrow)
with CiffReader('/export/data/ir/OWS.EU/data/index/index.ciff.gz') as reader:
    # Header info: TBD
    h = reader.read_header()
    header = MessageToJson(h, **pbopt)
    con.execute('CREATE TABLE stats (num_docs BIGINT, avgdl DOUBLE);');
    con.execute(f'INSERT INTO stats VALUES ({h.num_docs}, {h.average_doclength});');

    # RecordBatches for postings to an Arrow Datastructure
    postings_rb = iter_posting_batches(reader)
    postings_rbr = pa.ipc.RecordBatchReader.from_batches(postings_schema, postings_rb)

    # Create a DuckDB table from the Arrow data
    con.execute("""
      CREATE TABLE ciff_postings AS SELECT * FROM postings_rbr;
    """);

    # RecordBatches for docs to an Arrow Datastructure
    docs_rb = iter_docs_batches(reader)
    docs_rbr = pa.ipc.RecordBatchReader.from_batches(docs_schema, docs_rb)

    # Create a DuckDB table from the Arrow data
    con.execute("""
      CREATE TABLE docs AS SELECT docid::BIGINT AS docid, collection_docid AS name, doclength::BIGINT AS len FROM docs_rbr;
    """);

#
# Transform schema of the postings information (using DuckDB)
#
# Note: 
# Dropping cf here because DuckDB FTS does not use it, should be a CMDLINE option?
# Simply add cf as follows if needed: ... AS SELECT ...,cf ...
#
con.execute("""
  CREATE TABLE dict AS SELECT termid, term, df FROM ciff_postings;
""");
con.execute("""
  CREATE TABLE postings AS SELECT termid, unnest(postings, recursive := true) FROM ciff_postings;
""");
con.execute("DROP TABLE ciff_postings;")

#
# Query the index using the DuckDB tables
results = con.execute("SELECT termid FROM dict WHERE term LIKE '%radboud%' OR term LIKE '%university%'").arrow()
print(results)
results = con.execute("SELECT * FROM postings WHERE termid IN (select termid FROM dict WHERE term LIKE '%radboud%' OR term LIKE '%university%')").arrow()
print(results)

#
# Cleanup
con.close()
