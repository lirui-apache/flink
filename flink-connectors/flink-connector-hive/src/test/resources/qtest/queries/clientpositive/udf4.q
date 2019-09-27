CREATE TABLE dest1(c1 STRING) STORED AS TEXTFILE;

FROM src INSERT OVERWRITE TABLE dest1 SELECT '  abc  ' WHERE src.key = 86;

EXPLAIN
SELECT round(1.0), round(1.5), round(-1.5), floor(1.0), floor(1.5), floor(-1.5), sqrt(1.0), sqrt(-1.0), sqrt(0.0), ceil(1.0), ceil(1.5), ceil(-1.5), ceiling(1.0), rand(3), +3, -3, 1++2, 1+-2, 

~1, 
~CAST(1 AS TINYINT), 
~CAST(1 AS SMALLINT), 
~CAST(1 AS BIGINT), 

CAST(1 AS TINYINT) & CAST(2 AS TINYINT), 
CAST(1 AS SMALLINT) & CAST(2 AS SMALLINT), 
1 & 2, 
CAST(1 AS BIGINT) & CAST(2 AS BIGINT),

CAST(1 AS TINYINT) | CAST(2 AS TINYINT),
CAST(1 AS SMALLINT) | CAST(2 AS SMALLINT),
1 | 2,
CAST(1 AS BIGINT) | CAST(2 AS BIGINT),

CAST(1 AS TINYINT) ^ CAST(3 AS TINYINT),
CAST(1 AS SMALLINT) ^ CAST(3 AS SMALLINT),
1 ^ 3,
CAST(1 AS BIGINT) ^ CAST(3 AS BIGINT)

FROM dest1;

SELECT round(1.0), round(1.5), round(-1.5), floor(1.0), floor(1.5), floor(-1.5), sqrt(1.0), sqrt(-1.0), sqrt(0.0), ceil(1.0), ceil(1.5), ceil(-1.5), ceiling(1.0), rand(3), +3, -3, 1++2, 1+-2, 
~1, 
~CAST(1 AS TINYINT), 
~CAST(1 AS SMALLINT), 
~CAST(1 AS BIGINT), 

CAST(1 AS TINYINT) & CAST(2 AS TINYINT), 
CAST(1 AS SMALLINT) & CAST(2 AS SMALLINT), 
1 & 2, 
CAST(1 AS BIGINT) & CAST(2 AS BIGINT),

CAST(1 AS TINYINT) | CAST(2 AS TINYINT), 
CAST(1 AS SMALLINT) | CAST(2 AS SMALLINT), 
1 | 2, 
CAST(1 AS BIGINT) | CAST(2 AS BIGINT),

CAST(1 AS TINYINT) ^ CAST(3 AS TINYINT),
CAST(1 AS SMALLINT) ^ CAST(3 AS SMALLINT),
1 ^ 3,
CAST(1 AS BIGINT) ^ CAST(3 AS BIGINT)
 
FROM dest1;

drop table if exists dest1;
