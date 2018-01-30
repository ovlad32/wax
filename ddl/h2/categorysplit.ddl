create sequence if not exists SPLIT_COLUMN_DATA_SEQ;

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

drop table if exists category_split_data;
create table if not exists category_split_data(
 id bigint not null,
 category_split_id bigint not null,
 data text,
 constraint category_split_data_pk primary key(id)
);
/*

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
*/




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
);


drop table if exists fusion_column_group;
create table if not exists fusion_column_group (
  id bigint,
  table_info_id bigint,
  group_key VARCHAR(2000),
  row_count bigint,
  constraint fusion_column_group_pk primary key (id),
  constraint fusion_column_group_uq unique (table_info_id, group_key)
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
alter table column_info add column if not exists SOURCE_FUSION_COLUMN_INFO_ID bigint;
alter table column_info add column if not exists POSITION_IN_FUSION int;
alter table column_info add column if not exists TOTAL_IN_FUSION int;
alter table column_info add column if not exists FUSION_COLUMN_GROUP_ID bigint;
alter table column_info add column if not exists EMPTY_COUNT bigint;
alter table column_info add column if not exists FUSION_SEPARATOR varchar(1);

alter table column_info add column if not exists SOURCE_SLICE_COLUMN_INFO_ID bigint;

alter table table_info add column if not exists SOURCE_SLICE_TABLE_INFO_ID bigint;
alter table table_info add column if not exists CATEGORY_SPLIT_DATA_ID bigint;


create table if not exists column_feature_stats (
 column_info_id bigint,
 separator char(1),
 CONSTRAINT fusion_column_config_pk PRIMARY  KEY (column_info_id)
);

drop table if exists column_feature_stats;
create table if not exists column_feature_stats(
		 column_info_id bigint not null
		, key varchar(30)
		, byte_length int null
		, is_numeric bool  null
		, is_negative bool  null
		, is_integer bool null
		, total_count bigint
		, hash_unique_count bigint
		, item_unique_count bigint
		, min_sval varchar(4000)
		, max_sval varchar(4000)
		, min_fval float
		, max_fval float
		, moving_mean float
		, moving_stddev float
		, constraint column_feature_stats_pk  primary key(column_info_id, key)
);
