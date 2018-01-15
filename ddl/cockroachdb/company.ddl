create table if not exists Company (
 ID BIGINT NOT NULL,
 NAME VARCHAR(255) NOT NULL,
 constraint COPMPANY_PK primary key (ID)
);

insert into COMPANY(id,name) values (1,'Current Company');
commit;




