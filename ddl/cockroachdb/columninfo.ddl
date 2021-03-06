create table if not exists COLUMN_INFO(
  ID	BIGINT	 NOT NULL,
  AVG_LENGTH	DOUBLE PRECISION	 NULL,
  CHAR_LENGTH	BIGINT	 NULL,
  DATA_LENGTH	INTEGER	 NULL,
  DATA_PRECISION	INTEGER	 NULL,
  DATA_SCALE	INTEGER	 NULL,
  DATA_TYPE	VARCHAR(255)	 NOT NULL,
  DEFAULT_DATA	VARCHAR(255)	 NULL,
  DEFAULT_LENGTH	BIGINT	 NULL,
  DENSITY	DOUBLE PRECISION 	 NULL,
  DUMPED	BOOLEAN	 NULL,
  EMPTY	BOOLEAN	 NULL,
  HAS_NULLS	BOOLEAN	 NULL,
  HAS_WHITESES	BOOLEAN	 NULL,
  HASH_UNIQUE_COUNT	BIGINT	 NULL,
  INFERRED_TYPE	VARCHAR(255)	 NULL,
  IS_NULLABLE	BOOLEAN	 NULL,
  IS_NUMERIC_SEQUENCE	BOOLEAN	 NULL,
  MAX_LENGTH	BIGINT	 NULL,
  MAX_STRING_LENGTH	INTEGER	 NULL,
  MAX_VAL	VARCHAR(2000)	 NULL,
  MEAN	DOUBLE PRECISION	 NULL,
  MIN_STRING_LENGTH	INTEGER	 NULL,
  MIN_VAL	VARCHAR(2000)	 NULL,
  NAME	VARCHAR(255)	 NOT NULL,
  NUM_DISTINCT	BIGINT	 NULL,
  NUM_NULLS	BIGINT	 NULL,
  POSITION	INTEGER	 NOT NULL,
  REAL_TYPE	VARCHAR(255)	 NULL,
  SAMPLE_DATA_PATH	VARCHAR(255)	 NULL,
  SAMPLE_DATA_ROW_COUNT	INTEGER	 NULL,
  STD	DOUBLE PRECISION	 NULL,
  TOTAL_ROW_COUNT	BIGINT	 NULL,
  UNIQUE_ROW_COUNT	BIGINT	 NULL,
  TABLE_INFO_ID	BIGINT	 NULL,
  HAS_NUMERIC_CONTENT	BOOLEAN	 NULL,
  MIN_FVAL	DOUBLE PRECISION	 NULL,
  MAX_FVAL	DOUBLE PRECISION	 NULL,
  MIN_SVAL	VARCHAR(4000)	 NULL,
  MAX_SVAL	VARCHAR(4000)	 NULL,
  INTEGER_UNIQUE_COUNT	BIGINT	 NULL,
  MOVING_MEAN	DOUBLE PRECISION	 NULL,
  MOVING_STDDEV	DOUBLE PRECISION	 NULL,
  HAS_FLOAT_CONTENT	BOOLEAN	 NULL,
  POSITION_IN_PK	INTEGER	 NULL,
  TOTAL_IN_PK	INTEGER	 NULL,
  constraint COLUMN_INFO_PK PRIMARY KEY (ID)
)