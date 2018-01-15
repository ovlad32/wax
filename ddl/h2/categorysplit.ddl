create sequence if not exists SPLIT_COLUMN_DATA_SEQ

drop table if exists category_split;
create table if not exists category_split(
 id bigint not null,
 table_info_id bigint not null,
 status varchar(50),
 constraint category_split_pk primary key(id)
);

create table if exists category_split_column;
create table if not exists category_split_column(
 id bigint,
 category_split_id bigint,
 column_info_id bigint
 constraint category_split_column_pk(id)
);


drop table if  exists category_split_coldata;
create table if not exists category_split_coldata(
 id bigint,
 category_split_column_id bigint,
 data text,
 constraint category_split_coldata_pk primary key(id)
);

drop table if exists category_split_rowdata;
create table if not exists category_split_rowdata(
 id bigint,
 category_split_id bigint,
 data text,
 constraint category_split_rowdata_pk primary key(id)
);

drop table if  exists category_split_file;
create table if not exists category_split_file(
  id bigint,
  category_split_tbldata_id bigint,
  path_to_file varchar2(255),
  temp bool default false,
  indexed bool default false,
  zipped bool default false,
  row_count bigint,
  constraint category_split_file_pk primary key(id)
);

