REM ======================================================================
REM ===   Sql Script for Database : T0
REM ===
REM === Build : 242
REM ======================================================================

BEGIN
   -- Tables
   FOR o IN (SELECT table_name name FROM user_tables) LOOP
      dbms_output.put_line ('Dropping table ' || o.name || ' with dependencies');
      execute immediate 'drop table ' || o.name || ' cascade constraints';
   END LOOP;

   -- Sequences
   FOR o IN (SELECT sequence_name name FROM user_sequences) LOOP
      dbms_output.put_line ('Dropping sequence ' || o.name);
      execute immediate 'drop sequence ' || o.name;
   END LOOP;

   -- Triggers
   FOR o IN (SELECT trigger_name name FROM user_triggers) LOOP
      dbms_output.put_line ('Dropping trigger ' || o.name);
      execute immediate 'drop trigger ' || o.name;
   END LOOP;

   -- Synonyms
   FOR o IN (SELECT synonym_name name FROM user_synonyms) LOOP
      dbms_output.put_line ('Dropping synonym ' || o.name);
      execute immediate 'drop synonym ' || o.name;
   END LOOP;
END;
/
REM ======================================================================

CREATE TABLE run_status
  (
    id      int           not null,
    status  varchar(20)   unique not null,
    primary key(id)
  );

REM ======================================================================

CREATE TABLE trigger_segment_status
  (
    id      int           not null,
    status  varchar(20)   unique not null,
    primary key(id)
  );

REM ======================================================================

CREATE TABLE trigger_label
  (
    id     int            not null,
    label  varchar(255)   unique not null,
    primary key(id)
  );

REM ======================================================================

CREATE TABLE primary_dataset
  (
    dataset_id  int            not null,
    name        varchar(255)   not null,
    algorithm   varchar(255),
    event_size  int,
    primary key(dataset_id),
    unique(name)
  );

REM ======================================================================

CREATE TABLE stream
  (
    id    int            not null,
    name  varchar(255)   unique not null,
    primary key(id)
  );

REM ======================================================================

CREATE TABLE version
  (
    schema_version  varchar(10),
    description     varchar(100),
    primary key(schema_version)
  );

REM ======================================================================

CREATE TABLE repacked_status
  (
    id      int           not null,
    status  varchar(20)   unique not null,
    primary key(id)
  );

REM ======================================================================

CREATE TABLE reconstructed_status
  (
    id      int           not null,
    status  varchar(20)   unique not null,
    primary key(id)
  );

REM ======================================================================

CREATE TABLE export_status
  (
    id      int           not null,
    status  varchar(20)   unique not null,
    primary key(id)
  );

REM ======================================================================

CREATE TABLE job_status
  (
    id      int           not null,
    status  varchar(10)   unique not null,
    primary key(id)
  );

REM ======================================================================

CREATE TABLE block_status
  (
    id      int           not null,
    status  varchar(10)   unique not null,
    primary key(id)
  );

REM ======================================================================

CREATE TABLE block
  (
    block_id    int            not null,
    dataset_id  int            not null,
    block_name  varchar(255),
    data_tier   varchar(255),
    status      int            not null,
    primary key(block_id)
  );

REM ======================================================================

CREATE TABLE block_parentage
  (
    input_id   int   unique not null,
    output_id  int   unique not null,
    unique(input_id,output_id)
  );

REM ======================================================================

CREATE TABLE run
  (
    run_id       int            not null,
    start_time   int,
    end_time     int,
    process      varchar(255),
    version      varchar(255),
    run_status   int,
    last_streamer int,
    primary key(run_id)
  );

REM ======================================================================

CREATE TABLE run_trig_assoc
  (
    run_id      int   not null,
    trigger_id  int   not null,
    primary key(run_id,trigger_id)
  );

REM ======================================================================

CREATE TABLE lumi_section
  (
    run_id      int   not null,
    lumi_id     int   not null,
    start_time  int,
    primary key(run_id,lumi_id)
  );

REM ======================================================================

CREATE TABLE streamer
  (
    streamer_id     int             not null,
    run_id          int             not null,
    lumi_id         int             not null,
    start_time      int,
    insert_time     int,
    filesize        int,
    events          int,
    lfn             varchar(1000)   unique not null,
    indexpfn        varchar(1000),
    indexpfnbackup  varchar(1000),
    streamname      varchar(100),
    exportable      smallint,
    splitable       smallint,
    primary key(streamer_id)
  );

REM ======================================================================

CREATE TABLE trigger_segment
  (
    dataset_id    int   not null,
    streamer_id   int   not null,
    lumi_id       int   not null,
    run_id        int   not null,
    segment_size  int   not null,
    status        int,
    primary key(dataset_id,streamer_id)
  );

REM ======================================================================

CREATE TABLE trig_dataset_assoc
  (
    trig_id     int   not null,
    dataset_id  int   not null,
    run_id      int   not null,
    primary key(trig_id,dataset_id,run_id)
  );

REM ======================================================================

CREATE TABLE dataset_run_stream_assoc
  (
    dataset_id  int   not null,
    run_id      int   not null,
    stream_id   int,
    primary key(dataset_id,run_id)
  );

REM ======================================================================

CREATE TABLE repack_job_def
  (
    job_id           int   not null,
    job_status       int   not null,
    definition_time  int,
    primary key(job_id)
  );

REM ======================================================================

CREATE TABLE job_dataset_streamer_assoc
  (
    job_id       int   not null,
    dataset_id   int   not null,
    streamer_id  int   not null,
    primary key(job_id,dataset_id,streamer_id)
  );

REM ======================================================================

CREATE TABLE merge_job_def
  (
    job_id           int   not null,
    job_status       int   not null,
    definition_time  int,
    primary key(job_id)
  );

REM ======================================================================

CREATE TABLE repacked
  (
    repacked_id    int            not null,
    run_id         int            not null,
    filesize       int,
    events         int,
    cksum          varchar(255),
    lfn            varchar(255)   unique not null,
    dataset_id     int            not null,
    status         int,
    block_id       int,
    export_status  int            not null,
    primary key(repacked_id)
  );

REM ======================================================================

CREATE TABLE repack_lumi_assoc
  (
    lumi_id      int   not null,
    run_id       int   not null,
    repacked_id  int   not null,
    primary key(lumi_id,run_id,repacked_id)
  );

REM ======================================================================

CREATE TABLE repack_streamer_assoc
  (
    streamer_id  int   not null,
    repacked_id  int   not null,
    primary key(streamer_id,repacked_id)
  );

REM ======================================================================

CREATE TABLE repacked_merge_parentage
  (
    input_id   int   not null,
    output_id  int   not null,
    unique(input_id,output_id)
  );

REM ======================================================================

CREATE TABLE reconstructed
  (
    reconstructed_id  int            not null,
    filesize          int,
    cksum             varchar(255),
    events            int,
    lfn               varchar(255)   unique not null,
    status            int            not null,
    run_id            int            not null,
    dataset_id        int            not null,
    block_id          int,
    export_status     int            not null,
    primary key(reconstructed_id)
  );

REM ======================================================================

CREATE TABLE promptreco_job_def
  (
    job_id           int,
    job_status       int   not null,
    definition_time  int,
    primary key(job_id)
  );

REM ======================================================================

CREATE TABLE repacked_reco_parentage
  (
    input_id   int   not null,
    output_id  int   not null,
    unique(input_id,output_id)
  );

REM ======================================================================

CREATE TABLE reco_merge_parentage
  (
    input_id   int   not null,
    output_id  int   not null,
    unique(input_id,output_id)
  );

REM ======================================================================

CREATE TABLE active_block
  (
    block_id    int            not null,
    dataset_id  int            not null,
    run_id      int            not null,
    data_tier   varchar(255)   not null,
    primary key(block_id),
    unique(dataset_id, run_id, data_tier)
  );

REM ======================================================================

CREATE TABLE block_run_assoc
  (
    run_id    int   not null,
    block_id  int   not null,
    unique(run_id,block_id)
  );

REM ======================================================================

CREATE TABLE merge_job_repack_assoc
  (
    repacked_id  int   not null,
    job_id       int   not null
  );

REM ======================================================================

CREATE TABLE promptreco_job_repack_assoc
  (
    repacked_id  int   not null,
    job_id       int   not null
  );

REM ======================================================================

REM  ================================ 
-- SEQUENCES
REM  ================================ 

CREATE SEQUENCE run_status_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE trigger_segment_status_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE trigger_label_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE primary_dataset_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE stream_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE version_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE repacked_status_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE reconstructed_status_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE export_status_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE job_status_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE block_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE block_parentage_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE run_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE run_trig_assoc_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE lumi_section_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE streamer_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE trigger_segment_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE trig_dataset_assoc_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE dataset_run_stream_assoc_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE repack_job_def_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE job_dataset_streamer_assoc_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE merge_job_def_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE repacked_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE repack_lumi_assoc_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE repack_streamer_assoc_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE repacked_merge_parentage_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE reconstructed_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE promptreco_job_def_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE repacked_reco_parentage_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE reco_merge_parentage_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE active_block_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE block_run_assoc_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


CREATE SEQUENCE merge_job_repack_assoc_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;

CREATE SEQUENCE ptreco_job_repack_assoc_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;


-- ============= Initializations

insert into job_status (id, status) values (1, 'New');
insert into job_status (id, status) values (2, 'Used');
insert into job_status (id, status) values (3, 'Success');
insert into job_status (id, status) values (4, 'Failure');

insert into trigger_segment_status (id, status) values (1, 'New');
insert into trigger_segment_status (id, status) values (2, 'Processing');
insert into trigger_segment_status (id, status) values (3, 'Repacked');
insert into trigger_segment_status (id, status) values (4, 'RepackFailed');

insert into run_status (id, status) values (1, 'Active');
insert into run_status (id, status) values (2, 'Processing');
insert into run_status (id, status) values (3, 'Complete');
insert into run_status (id, status) values (4, 'CloseOutMerge');
insert into run_status (id, status) values (5, 'CloseOutRepack');
insert into run_status (id, status) values (6, 'CloseOut');

insert into repacked_status (id, status) values (1, 'Mergeable');
insert into repacked_status (id, status) values (2, 'MergeQueued');
insert into repacked_status (id, status) values (3, 'Merged');
insert into repacked_status (id, status) values (4, 'Repacked');
insert into repacked_status (id, status) values (5, 'ReconstructionQueued');
insert into repacked_status (id, status) values (6, 'Reconstructed');
insert into repacked_status (id, status) values (7, 'MergeFailed');
insert into repacked_status (id, status) values (8, 'ReconstructionFailed');

insert into export_status (id, status) values (1, 'NotExportable');
insert into export_status (id, status) values (2, 'Exportable');
insert into export_status (id, status) values (3, 'InFlight');
insert into export_status (id, status) values (4, 'Exported');

insert into reconstructed_status (id, status) values (1, 'Mergeable');
insert into reconstructed_status (id, status) values (2, 'MergeQueued');
insert into reconstructed_status (id, status) values (3, 'Merged');
insert into reconstructed_status (id, status) values (4, 'Reconstructed');
insert into reconstructed_status (id, status) values (5, 'MergeFailed');

insert into block_status (id, status) values (1, 'Active');
insert into block_status (id, status) values (2, 'Closed');
insert into block_status (id, status) values (3, 'Exported');

commit;



