## Limitations
The SQL extractor module is for demo purposes only and is designed to showcase the capabilities of 
Audit Logs processing using ZetaSQL. It is extensible by design, but is not a complete library. 

The parser is has following limitations (non-exhaustive)
* Only LOAD, QUERY and COPY jobs are supported.
* Need to specify Fully Qualified Table name in a query (e.g. project-id.dataset_id.table_id)
* DDL statements unsupported due to limitations of ZetaSQL (2020.04.01)
* Only simple functions and aggregate function extraction is implemented as proof-of-concept, 
   * complex functions (e.g. Window Aggregation) and operations are not implemented.
