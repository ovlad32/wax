create sequence if not exists SPLIT_COLUMN_DATA_SEQ

drop table if exists category_split;
create table if not exists category_split(
 id bigint not null,
 table_info_id bigint not null,
 status varchar(50),
 constraint category_split_pk primary key(id)
);

drop table if exists category_split_column;
create table if not exists category_split_column(
 id bigint not null,
 category_split_id bigint not null,
 column_info_id bigint not null,
 constraint category_split_column_pk  primary key(id)
);


drop table if exists category_split_coldata;
create table if not exists category_split_coldata(
 id bigint not null,
 category_split_column_id bigint not null,
 data text not null,
 constraint category_split_coldata_pk primary key(id)
);

drop table if exists category_split_rowdata;
create table if not exists category_split_rowdata(
 id bigint not null,
 category_split_id bigint not null,
 data text,
 constraint category_split_rowdata_pk primary key(id)
);


drop table if  exists category_split_file;
create table if not exists category_split_file(
  id bigint not null,
  category_split_tbldata_id bigint not null,
  path_to_file varchar2(255),
  perma bool default false,
  indexed bool default false,
  zipped bool default false,
  row_count bigint,
  constraint category_split_file_pk primary key(id)
);


select * from CATEGORY_SPLIT_COLUMN



create sequence if not exists app_node_seq;

drop table if exists app_node;

create table if not exists app_node (
 id bigint,
 hostname varchar(255),
 address varchar(255),
 last_heartbeat varchar(50),
 state varchar(50),
 role  varchar(50),
 constraint app_node_pk primary key (id)
)

create table fusion_column_group (
  id bigint,
  column_info_id bigint,
  group_tuples text,
  constraint  fusion_column_group_pk primary key (id)
);

alter table column_info add column if not exists NUMERIC_COUNT bigint;
alter table column_info add column if not exists min_fval float;
alter table column_info add column if not exists max_fval float;
alter table column_info add column if not exists min_sval varchar(4000);
alter table column_info add column if not exists max_sval varchar(4000);
alter table column_info add column if not exists integer_unique_count bigint;
alter table column_info add column if not exists integer_count bigint;
alter table column_info add column if not exists moving_mean float;
alter table column_info add column if not exists moving_stddev float;
alter table column_info add column if not exists POSITION_IN_PK int;
alter table column_info add column if not exists TOTAL_IN_PK int;
alter table column_info add column if not exists SOURCE_FUSION_COLUMN_ID bigint;
alter table column_info add column if not exists POSITION_IN_FUSION int;
alter table column_info add column if not exists TOTAL_IN_FUSION int;
alter table column_info add column if not exists FUSION_COLUMN_GROUP_ID bigint;
alter table column_info add column if not exists EMPTY_COUNT bigint;
