
create table if not exists column_feature_stats(
    column_info_id bigint not null
   , key varchar(20) not null
   , byte_length bigint
   , is_numeric boolean
   , is_negative boolean
   , is_integer boolean
   , non_null_count bigint
   , hash_unique_count bigint
   , item_unique_count bigint
   , min_sval varchar(4000)
   , max_sval varchar(4000)
   , min_fval DOUBLE PRECISION
   , max_fval DOUBLE PRECISION
   , constraint column_feature_stats_pk primary key(column_info_id, key)
);
