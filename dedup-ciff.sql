--
-- Dealing with documents that occur multiple times in the CIFF file
--
-- We assume that their document representations are identical;
-- which would be incorrect if the document changed between different captures
--
-- (A different version using SQL windowing functions or maybe an ASOF join 
--    on position could instead take the last document capture.)
--

create schema fixedciff;
use ows;

-- Intermediary table with the data we need to correct the other tables:
-- the postings of duplicate documents with their required df correction
create or replace temporary table tdupdoc as 
  select termid, docid, any_value(tf) as tf, count(docid) - 1 as deltadf 
  from postings group by termid, docid having count(docid)>1;

-- Dictionary with corrected df values
create or replace table fixedciff.dict as 
  select termid,term,any_value(df) as df from (
    select d.termid, d.term, (df - coalesce(deltadf, 0)) as df 
    from ows.dict d left outer join tdupdoc on (d.termid = tdupdoc.termid) ) 
  group by termid,term order by termid;

-- Docs without duplicates
-- Keeping max(len) is an arbitrary choice given the assumptions stated,
-- but it avoids a length 0 for a document that has postings from another capture.
create or replace table fixedciff.docs as
  select distinct docid, name, max(len) as len from ows.docs 
  group by docid, name;

-- Postings without the additional entries of duplicate documents
-- Alternative 1:
-- Select the postings corresponding to unique documents; 
-- union these with the postings from duplicate ones;
-- ensure the order by term identifier.
--
-- create or replace table fixedciff.postings as 
--  select * from (
--      select termid, docid, tf from ows.postings p
--      where not exists (
--        select termid, docid from tdupdoc 
--	where termid = p.termid and docid = p.docid
--      )
--    union
--      select termid, docid, tf from tdupdoc )
--  order by termid, docid;
--

-- Postings without the additional entries of duplicate documents
-- More efficient alternative 2:
-- Using group-by to keep only one posting per term-doc pair:
create or replace table fixedciff.postings as
  select termid, docid, any_value(tf)
  from ows.postings group by termid, docid
  order by termid, docid;

drop table tdupdoc;
