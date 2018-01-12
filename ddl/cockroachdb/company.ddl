create table if not exists edm.Company (
 ID BIGINT NOT NULL,
 NAME VARCHAR(255) NOT NULL,
 constraint COPMPANY_PK primary key (ID)
);

insert into edm.COMPANY values (1,'Current Company');
commit;




