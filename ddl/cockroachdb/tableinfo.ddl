

create table if not exists EDM.TABLE_INFO(
  ID	BIGINT	NOT NULL,
  DATABASE_NAME	VARCHAR(255)	NOT NULL,
  DUMPED	BOOLEAN	NULL,
  INDEXED	BOOLEAN	NULL,
  NAME	VARCHAR(255)	NOT NULL,
  PATH_TO_DATA_DIR	VARCHAR(255)	NULL,
  PATH_TO_FILE	VARCHAR(255)	NULL,
  ROW_COUNT	INTEGER	NULL,
  SAMPLE_DATA_PATH	VARCHAR(255)	NULL,
  SCHEMA_NAME	VARCHAR(255)	NOT NULL,
  SIZE_INKB	BIGINT	NULL,
  UNIQUE_NAME	VARCHAR(255)	NOT NULL,
  METADATA_ID	BIGINT	NULL,
  constraint TABLE_INFO_PK PRIMARY KEY (ID)
);

