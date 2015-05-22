REM ======================================================================
REM ===   Sql Script for Database : T0
REM ===
REM === Build : 242
REM ======================================================================

CREATE TABLE processed_dataset
(
  id int not null,
  name varchar(255) not null,
  primary key (id),
  unique (name)
); 

REM ======================================================================

CREATE TABLE primary_dataset
(
  id int not null,
  name varchar(255) not null,
  primary key (id),
  unique (name)
); 

REM ======================================================================

CREATE TABLE data_tier
(
  id int not null,
  name varchar(255) not null,
  primary key (id),
  unique (name)
); 

REM ======================================================================

CREATE TABLE dataset_path
(
    id int not null,
    primary_dataset int not null,
    processed_dataset int not null,
    data_tier int not null,
    primary key (id),
    unique (primary_dataset, processed_dataset, data_tier)
);

REM ======================================================================

CREATE TABLE express_file_info
(
    file_id int not null,
    stream_id int not null,
    data_tier int not null,
    processed_dataset int not null,
    primary_dataset int,
    primary key (file_id)
);

REM ======================================================================

CREATE TABLE wmbs_file_dataset_path_assoc
(
    file_id int not null,
    dataset_path_id int not null,
    primary key (file_id)
);

REM ======================================================================

CREATE TABLE wmbs_file_block_assoc
(
    file_id int not null,
    block_id int not null,
    primary key (file_id)
);

REM ======================================================================

CREATE TABLE run_status
  (
    id      int           not null,
    status  varchar(25)   unique not null,
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

CREATE TABLE stream
  (
    id    int            not null,
    name  varchar(255)   unique not null,
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

CREATE TABLE block_migrate_status
  (
    id      int           not null,
    status  varchar(25)   unique not null,
    primary key(id)
  );

REM ======================================================================

CREATE TABLE block_delete_status
  (
    id      int           not null,
    status  varchar(25)   unique not null,
    primary key(id)
  );

REM ======================================================================

CREATE TABLE block
  (
    id                int           not null,
    dataset_path_id   int           not null,
    name              varchar(500),
    block_size        int           not null,
    file_count        int           not null,
    status            int           not null,
    migrate_status    int default 1 not null,
    delete_status     int default 1 not null,  
    export_start_time int,
    export_end_time   int,
    primary key(id)
  );

REM ======================================================================

CREATE TABLE block_parentage
  (
    input_id   int   not null,
    output_id  int   not null,
    unique(input_id,output_id)
  );

REM ======================================================================

CREATE TABLE run
  (
    run_id             int            not null,
    run_version        int            not null,
    repack_version     int            not null,
    express_version    int            not null,
    hltkey             varchar(255)   not null,
    run_status         int            not null,
    start_time         int            not null,
    end_time           int,
    process            varchar(255),
    acq_era            varchar(255),
    reco_started       int,
    last_streamer      int,
    last_updated       int,
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

CREATE TABLE lumi_section_closed
  (
    run_id      int   not null,
    lumi_id     int   not null,
    stream_id   int   not null,
    insert_time int   not null,
    primary key(run_id,lumi_id,stream_id)
  );

REM ======================================================================

CREATE TABLE lumi_section_express_done
  (
    run_id      int   not null,
    lumi_id     int   not null,
    stream_id   int   not null,
    insert_time int   not null,
    primary key(run_id,lumi_id,stream_id)
  );

REM ======================================================================

CREATE TABLE streamer
  (
    streamer_id     int           not null,
    run_id          int           not null,
    lumi_id         int           not null,
    insert_time     int           not null,
    filesize        int           not null,
    events          int           not null,
    lfn             varchar(1000) unique not null,
    exportable      int           not null,
    stream_id       int,
    indexpfn        varchar(1000),
    indexpfnbackup  varchar(1000),
    primary key(streamer_id)
  );

REM ======================================================================

CREATE TABLE streamer_used
  (
    streamer_id      int           not null,
    insert_time      int           not null,
    primary key(streamer_id)
  );

REM ======================================================================

CREATE TABLE streamer_exported
  (
    streamer_id      int           not null,
    insert_time      int           not null,
    primary key(streamer_id)
  );

REM ======================================================================

CREATE TABLE streamer_deletable
  (
    streamer_id      int           not null,
    insert_time      int           not null,
    primary key(streamer_id)
  );

REM ======================================================================

CREATE TABLE streamer_deleted
  (
    streamer_id      int           not null,
    insert_time      int           not null,
    primary key(streamer_id)
  );

REM ======================================================================

CREATE TABLE repack_type
   (
     id int not null,
     repack_type varchar(255) not null,
     primary key(id),
     unique (repack_type)
   );

REM ======================================================================

CREATE TABLE trigger_segment
  (
    primary_dataset_id int   not null,
    streamer_id        int   not null,
    lumi_id            int   not null,
    run_id             int   not null,
    segment_size       int   not null,
    status             int,
    primary key(primary_dataset_id, streamer_id)
  );

REM ======================================================================

CREATE TABLE trig_dataset_assoc
  (
    trig_id             int   not null,
    primary_dataset_id  int   not null,
    run_id              int   not null,
    primary key(trig_id, primary_dataset_id, run_id)
  );

REM ======================================================================

CREATE TABLE dataset_run_stream_assoc
  (
    primary_dataset_id  int   not null,
    run_id              int   not null,
    stream_id           int,
    primary key(primary_dataset_id, run_id)
  );

REM ======================================================================

CREATE TABLE repack_job_def
  (
    job_id           int   not null,
    job_status       int   not null,
    definition_time  int,
    completion_time  int,
    retry_count      int   default 0,
    log_archive      varchar(255),
    primary key(job_id)
  );

REM ======================================================================

CREATE TABLE express_job_def
  (
    job_id           int   not null,
    job_status       int   not null,
    definition_time  int,
    completion_time  int,
    retry_count      int   default 0,
    log_archive      varchar(255),
    primary key(job_id)
  );

REM ======================================================================

CREATE TABLE job_streamer_dataset_assoc
  (
    job_id               int   not null,
    streamer_id          int   not null,
    primary_dataset_id   int   not null,
    primary key(job_id, streamer_id, primary_dataset_id)
  );

REM ======================================================================

CREATE TABLE job_streamer_assoc
  (
    job_id               int   not null,
    streamer_id          int   not null,
    primary key(job_id, streamer_id)
  );

REM ======================================================================

CREATE TABLE repack_streamer_assoc
  (
    streamer_id  int   not null,
    repacked_id  int   not null,
    primary key(streamer_id, repacked_id)
  );

REM ======================================================================

CREATE TABLE active_block
  (
    block_id               int            not null,
    dataset_path_id  int            not null,
    run_id           int            not null,
    primary key(block_id),
    unique(dataset_path_id, run_id)
  );

REM ======================================================================

CREATE TABLE block_run_assoc
  (
    run_id    int   not null,
    block_id  int   not null,
    unique(run_id,block_id)
  );

REM ======================================================================

CREATE TABLE t0_config
 (
   run_id         int          not null,
   config_version varchar(255) not null,
   primary key(run_id)
 );

REM ======================================================================

CREATE TABLE event_scenario 	 
  ( 	 
    id       int          not null, 	 
    scenario varchar(255) not null, 	 
    primary key(id), 	 
    unique (scenario) 	 
  ); 	 
REM ====================================================================== 	 

CREATE TABLE scenario_config 	 
  ( 	 
    run_id              int not null, 	 
    primary_dataset_id  int not null, 	 
    scenario_id         int not null, 	 
    primary key (run_id, primary_dataset_id) 	 
  ); 	 
	  	 
REM ======================================================================

CREATE TABLE run_stream_style_assoc
 (
   run_id    int not null,
   stream_id int not null,
   style_id  int not null,
   primary key(run_id, stream_id)
 );

REM ======================================================================

CREATE TABLE processing_style
 (
   id    int          not null,
   name  varchar(255) not null,
   primary key(id),
   unique (name)
 );

REM ======================================================================

CREATE TABLE cmssw_version
 (
   id   int          not null,
   name varchar(255) not null,
   primary key(id),
   unique (name)
 );
REM ======================================================================

CREATE TABLE repack_config
 (
   run_id               int not null,
   primary_dataset_id   int not null,
   proc_version         varchar(255),
   primary key (run_id, primary_dataset_id)
 );

REM ======================================================================

CREATE TABLE reco_config
 (
   run_id             int not null,
   primary_dataset_id int not null,
   do_reco            int not null,
   cmssw_version_id   int not null,
   global_tag         varchar(255),
   config_url         varchar(255),
   proc_version       varchar(255),
   pset_hash          varchar(700),
   branch_hash        varchar(700),
   primary key (run_id, primary_dataset_id)
 );

REM ======================================================================

CREATE TABLE t1skim_config
 (
   run_id             int not null,
   primary_dataset_id int not null,
   data_tier_id       int not null,
   cmssw_version_id   int not null,
   two_file_read      int not null,
   skim_name          varchar(255),
   config_url         varchar(255),
   proc_version       varchar(255),
   primary key (run_id, primary_dataset_id, data_tier_id, skim_name)
 );

REM ======================================================================

CREATE TABLE express_config
 (
   run_id                  int          not null,
   stream_id               int          not null,
   processing_config_url   varchar(255) not null,
   splitInProcessing       int          not null,
   proc_version            varchar(255) not null,
   alcamerge_config_url    varchar(255),
   global_tag              varchar(255),
   primary key (run_id, stream_id)
 );

REM ======================================================================

CREATE TABLE run_stream_tier_assoc
 (
   run_id       int not null,
   stream_id    int not null,
   data_tier_id int not null,
   primary key (run_id, stream_id, data_tier_id)
 );

REM ======================================================================

CREATE TABLE alca_config
 (
   run_id             int not null,
   primary_dataset_id int not null,
   do_alca            int not null,
   cmssw_version_id   int not null,
   config_url         varchar(255),
   proc_version       varchar(255),
   pset_hash          varchar(700),
   branch_hash        varchar(700),
   primary key (run_id, primary_dataset_id)
 );
 
REM ======================================================================

CREATE TABLE wmbs_publish_config
 (
   run_id             int not null,
   primary_dataset_id int not null,
   do_wmbs_publish    int not null,
   data_tiers_to      varchar(1000),
   primary key (run_id, primary_dataset_id)
 );
 
REM ======================================================================

CREATE TABLE dqm_config
 (
   run_id             int not null,
   primary_dataset_id int not null,
   do_dqm             int not null,
   cmssw_version_id   int not null,
   config_url         varchar(255),
   proc_version       varchar(255),
   pset_hash          varchar(700),
   branch_hash        varchar(700),
   primary key (run_id, primary_dataset_id)
 );
 
REM ======================================================================
 
CREATE TABLE phedex_subscription
 (
   run_id             int not null,
   primary_dataset_id int not null,
   data_tier_id       int not null,
   node_id            int not null,
   custodial_flag     int not null,
   request_only       varchar(1) default 'y' not null,
   priority           varchar(10) default 'normal' not null,
   primary key (run_id, primary_dataset_id, node_id, data_tier_id)
 );

REM ======================================================================

CREATE TABLE phedex_subscription_made
 (
   dataset_path_id int not null,
   node_id            int not null,
   requester_id       int not null,
   primary key (dataset_path_id, node_id)
 );

REM ======================================================================

CREATE TABLE storage_node
 (
   id   int not null,
   name varchar(255),
   primary key (id),
   unique (name)
 );
 
REM =======================================================================

CREATE TABLE repack_hltdebug_parentage
  (
    repacked_id int not null,
    hltdebug_id int not null,
    unique (repacked_id, hltdebug_id)
  );

REM ======================================================================

CREATE TABLE component_heartbeat
  (
    name varchar(50) not null,
    last_updated int, 
    primary key (name)
  );

REM ======================================================================

ALTER TABLE dataset_path ADD CONSTRAINT
    dataset_path_primary_FK foreign key(primary_dataset) references primary_dataset(id)
/
ALTER TABLE dataset_path ADD CONSTRAINT
    dataset_path_processed_FK foreign key(processed_dataset) references processed_dataset(id)
/
ALTER TABLE dataset_path ADD CONSTRAINT
    dataset_path_data_tier_FK foreign key(data_tier) references data_tier(id)
/
ALTER TABLE express_file_info ADD CONSTRAINT
    express_file_wmbs_file_FK foreign key (file_id) references wmbs_file_details(id)
/
ALTER TABLE express_file_info ADD CONSTRAINT
    express_file_stream_FK foreign key (stream_id) references stream(id)
/
ALTER TABLE express_file_info ADD CONSTRAINT
    express_file_data_tier_FK foreign key (data_tier) references data_tier(id)
/
ALTER TABLE express_file_info ADD CONSTRAINT
    express_file_pc_dataset_FK foreign key (processed_dataset) references processed_dataset(id)
/
ALTER TABLE express_file_info ADD CONSTRAINT
    express_file_dataset_FK foreign key (primary_dataset) references primary_dataset(id)
/
ALTER TABLE wmbs_file_dataset_path_assoc ADD CONSTRAINT
    wmbs_file_dataset_path_file_FK foreign key (file_id) references wmbs_file_details(id)
/
ALTER TABLE wmbs_file_dataset_path_assoc ADD CONSTRAINT
    wmbs_file_dataset_path_FK foreign key (dataset_path_id) references dataset_path(id)
/
ALTER TABLE wmbs_file_block_assoc ADD CONSTRAINT
    wmbs_file_block_assoc_file_FK foreign key (file_id) references wmbs_file_details(id)
/
ALTER TABLE wmbs_file_block_assoc ADD CONSTRAINT
    wmbs_file_block_assoc_block_FK foreign key (block_id) references block(id)
/
ALTER TABLE block ADD CONSTRAINT
    block_dataset_path_id_FK foreign key (dataset_path_id) references dataset_path(id)
/
ALTER TABLE block ADD CONSTRAINT
    block_status_FK foreign key (status) references block_status(id)
/
ALTER TABLE block ADD CONSTRAINT
    block_migrate_status_FK foreign key (migrate_status) references block_migrate_status(id)
/
ALTER TABLE block_parentage ADD CONSTRAINT
    block_parentage_input_id_FK foreign key (input_id) references block(id)
/
ALTER TABLE block_parentage ADD CONSTRAINT
    block_parentage_output_id_FK foreign key (output_id) references block(id)
/
ALTER TABLE run ADD CONSTRAINT
    run_run_status_FK foreign key (run_status) references run_status(id)
/
ALTER TABLE run ADD CONSTRAINT
    run_last_streamer_FK foreign key (last_streamer) references streamer(streamer_id)
/
ALTER TABLE run ADD CONSTRAINT
    run_run_version_FK foreign key (run_version) references cmssw_version(id)
/
ALTER TABLE run ADD CONSTRAINT
    run_repack_version_FK foreign key (repack_version) references cmssw_version(id)
/
ALTER TABLE run ADD CONSTRAINT
    run_express_version_FK foreign key (express_version) references cmssw_version(id)
/
ALTER TABLE run_trig_assoc ADD CONSTRAINT
    run_trig_assoc_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE run_trig_assoc ADD CONSTRAINT
    run_trig_assoc_trigger_id_FK foreign key (trigger_id) references trigger_label(id)
/
ALTER TABLE lumi_section ADD CONSTRAINT
    lumi_section_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE lumi_section_closed ADD CONSTRAINT
    lumi_sec_clos_rl_FK foreign key (run_id,lumi_id) references lumi_section(run_id,lumi_id)
/
ALTER TABLE lumi_section_closed ADD CONSTRAINT
    lumi_sec_clos_st_FK foreign key (stream_id) references stream(id)
/
ALTER TABLE lumi_section_express_done ADD CONSTRAINT
    lumi_sec_exp_do_rl_FK foreign key (run_id,lumi_id) references lumi_section(run_id,lumi_id)
/
ALTER TABLE lumi_section_express_done ADD CONSTRAINT
    lumi_sec_exp_do_st_FK foreign key (stream_id) references stream(id)
/
ALTER TABLE streamer ADD CONSTRAINT
    streamer_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE streamer ADD CONSTRAINT
    streamer_run_lumi_id_FK foreign key (run_id, lumi_id) references lumi_section(run_id, lumi_id) 
/
ALTER TABLE streamer ADD CONSTRAINT
    streamer_stream_id_FK foreign key (stream_id) references stream(id)
/
ALTER TABLE streamer_used ADD CONSTRAINT
    streamer_used_streamer_id_FK foreign key (streamer_id) references streamer(streamer_id)
/
ALTER TABLE streamer_exported ADD CONSTRAINT
    streamer_exported_id_FK foreign key (streamer_id) references streamer(streamer_id)
/
ALTER TABLE streamer_deletable ADD CONSTRAINT
    streamer_deletable_id_FK foreign key (streamer_id) references streamer(streamer_id)
/
ALTER TABLE streamer_deleted ADD CONSTRAINT
    streamer_deleted_id_FK foreign key (streamer_id) references streamer(streamer_id)
/
ALTER TABLE trigger_segment ADD CONSTRAINT
    trigger_segment_dataset_id_FK foreign key (primary_dataset_id) references primary_dataset(id)
/
ALTER TABLE trigger_segment ADD CONSTRAINT
    trigger_segment_streamer_id_FK foreign key (streamer_id) references streamer(streamer_id)
/
ALTER TABLE trigger_segment ADD CONSTRAINT
    trigger_seg_run_lumi_FK foreign key (run_id, lumi_id) references lumi_section(run_id, lumi_id)
/
ALTER TABLE trigger_segment ADD CONSTRAINT
    trigger_segment_status_FK foreign key (status) references trigger_segment_status(id)
/
ALTER TABLE trig_dataset_assoc ADD CONSTRAINT
    trig_dataset_dataset_id_FK foreign key (primary_dataset_id) references primary_dataset(id)
/
ALTER TABLE trig_dataset_assoc ADD CONSTRAINT
    trig_dataset_assoc_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE trig_dataset_assoc ADD CONSTRAINT
    trig_dataset_assoc_trig_id_FK foreign key(trig_id) references trigger_label(id)
/
ALTER TABLE dataset_run_stream_assoc ADD CONSTRAINT
    dataset_run_dataset_id_FK foreign key (primary_dataset_id) references primary_dataset(id)
/
ALTER TABLE dataset_run_stream_assoc ADD CONSTRAINT
    dataset_run_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE dataset_run_stream_assoc ADD CONSTRAINT
    dataset_run_stream_id_FK foreign key (stream_id) references stream(id)
/
ALTER TABLE repack_job_def ADD CONSTRAINT
    repack_job_def_job_status_FK foreign key (job_status) references job_status(id)
/
ALTER TABLE job_streamer_dataset_assoc ADD CONSTRAINT
    job_streamer_dataset_job_id_FK foreign key (job_id) references repack_job_def(job_id) on delete CASCADE
/
ALTER TABLE job_streamer_dataset_assoc ADD CONSTRAINT
    job_streamer_dataset_id_FK foreign key (streamer_id) references streamer(streamer_id) on delete CASCADE
/
ALTER TABLE job_streamer_dataset_assoc ADD CONSTRAINT
    job_dataset_dataset_id_FK foreign key (primary_dataset_id) references primary_dataset(id)
/
ALTER TABLE job_streamer_assoc ADD CONSTRAINT 
    jobstreamerassocjobi_FK foreign key(job_id) references express_job_def(job_id) on delete CASCADE
/
ALTER TABLE job_streamer_assoc ADD CONSTRAINT
    jobstreamerassocdata_FK foreign key(streamer_id) references streamer(streamer_id) on delete CASCADE
/
ALTER TABLE repack_streamer_assoc ADD CONSTRAINT
    repack_streamer_streamer_FK foreign key (streamer_id) references streamer(streamer_id)
/
ALTER TABLE repack_streamer_assoc ADD CONSTRAINT
    repack_streamer_repacked_FK foreign key (repacked_id) references wmbs_file_details(id)
/
ALTER TABLE active_block ADD CONSTRAINT
    active_block_block_id_FK foreign key (block_id) references block(id)
/    
ALTER TABLE active_block ADD CONSTRAINT
    active_block_dataset_FK foreign key (dataset_path_id) references dataset_path(id)
/
ALTER TABLE active_block ADD CONSTRAINT
    active_block_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE block_run_assoc ADD CONSTRAINT
    block_run_assoc_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE block_run_assoc ADD CONSTRAINT
    block_run_assoc_block_id_FK foreign key (block_id) references block(id)
/
ALTER TABLE express_job_def ADD CONSTRAINT 
    express_job_def_job_status_FK foreign key(job_status) references job_status(id)
/
ALTER TABLE t0_config ADD CONSTRAINT
    t0_config_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE scenario_config ADD CONSTRAINT
    scenario_config_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE scenario_config ADD CONSTRAINT
    scenario_config_dataset_FK foreign key (primary_dataset_id) references primary_dataset(id)
/
ALTER TABLE scenario_config ADD CONSTRAINT
    scenario_config_scenario_FK foreign key (scenario_id) references event_scenario(id)
/
ALTER TABLE run_stream_style_assoc ADD CONSTRAINT
    run_str_sty_as_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE run_stream_style_assoc ADD CONSTRAINT
    run_str_sty_as_stream_id_FK foreign key (stream_id) references stream(id)
/
ALTER TABLE run_stream_style_assoc ADD CONSTRAINT
    run_str_sty_as_style_id_FK foreign key (style_id) references processing_style(id)
/
ALTER TABLE repack_config ADD CONSTRAINT
    repack_config_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE repack_config ADD CONSTRAINT
    repack_config_dataset_id_FK foreign key (primary_dataset_id) references primary_dataset(id)
/
ALTER TABLE reco_config ADD CONSTRAINT
    reco_config_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE reco_config ADD CONSTRAINT
    reco_config_dataset_id_FK foreign key (primary_dataset_id) references primary_dataset(id)
/
ALTER TABLE reco_config ADD CONSTRAINT
    reco_config_cmssw_FK foreign key (cmssw_version_id) references cmssw_version(id)
/
ALTER TABLE t1skim_config ADD CONSTRAINT
    t1skim_config_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE t1skim_config ADD CONSTRAINT
    t1skim_config_dataset_id_FK foreign key (primary_dataset_id) references primary_dataset(id)
/
ALTER TABLE t1skim_config ADD CONSTRAINT
    t1skim_config_data_tier_id_FK foreign key (data_tier_id) references data_tier(id)
/
ALTER TABLE t1skim_config ADD CONSTRAINT
    t1skim_config_cmssw_FK foreign key (cmssw_version_id) references cmssw_version(id)
/
ALTER TABLE express_config ADD CONSTRAINT
    express_config_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE express_config ADD CONSTRAINT
    express_config_stream_id_FK foreign key (stream_id) references stream(id)
/
ALTER TABLE run_stream_tier_assoc ADD CONSTRAINT
    rst_assoc_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE run_stream_tier_assoc ADD CONSTRAINT
    rst_assoc_stream_id_FK foreign key (stream_id) references stream(id)
/
ALTER TABLE run_stream_tier_assoc ADD CONSTRAINT
    rst_assoc_tier_id_FK foreign key (data_tier_id) references data_tier(id)
/
ALTER TABLE alca_config ADD CONSTRAINT
    alca_config_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE alca_config ADD CONSTRAINT
    alca_config_dataset_id_FK foreign key (primary_dataset_id) references primary_dataset(id)
/
ALTER TABLE alca_config ADD CONSTRAINT
    alca_config_cmssw_FK foreign key (cmssw_version_id) references cmssw_version(id)
/
ALTER TABLE dqm_config ADD CONSTRAINT
    dqm_config_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE dqm_config ADD CONSTRAINT
    dqm_config_dataset_id_FK foreign key (primary_dataset_id) references primary_dataset(id)
/
ALTER TABLE dqm_config ADD CONSTRAINT
    dqm_config_cmssw_version_id_FK foreign key (cmssw_version_id) references cmssw_version(id)
/
ALTER TABLE phedex_subscription ADD CONSTRAINT
    phedex_subscription_run_id_FK foreign key (run_id) references run(run_id)
/
ALTER TABLE phedex_subscription ADD CONSTRAINT
    phedex_sub_dataset_id_FK foreign key (primary_dataset_id) references primary_dataset(id)
/
ALTER TABLE phedex_subscription ADD CONSTRAINT
    phedex_sub_data_tier_id_FK foreign key (data_tier_id) references data_tier(id)
/
ALTER TABLE phedex_subscription ADD CONSTRAINT
    phedex_subscription_node_id_FK foreign key (node_id) references storage_node(id)
/
ALTER TABLE phedex_subscription_made ADD CONSTRAINT
    phedex_made_ds_path_id_FK foreign key (dataset_path_id) references dataset_path(id)
/
ALTER TABLE phedex_subscription_made ADD CONSTRAINT
    phedex_made_node_id_FK foreign key (node_id) references storage_node(id)
/
ALTER TABLE repack_hltdebug_parentage ADD CONSTRAINT
    repack_hltdebug_repacked_id_FK foreign key (repacked_id) references wmbs_file_details(id)
/
ALTER TABLE repack_hltdebug_parentage ADD CONSTRAINT
    repack_hltdebug_hltdebug_id_FK foreign key (hltdebug_id) references wmbs_file_details(id)
/
REM  ================================ 

-- SEQUENCES

CREATE SEQUENCE event_scenario_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;

CREATE SEQUENCE cmssw_version_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;

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

CREATE SEQUENCE repack_job_def_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;

CREATE SEQUENCE express_job_def_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;

CREATE SEQUENCE repack_type_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;

CREATE SEQUENCE storage_node_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;

CREATE SEQUENCE primary_dataset_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;

CREATE SEQUENCE processed_dataset_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;

CREATE SEQUENCE data_tier_SEQ
start with 100
increment by 1
nomaxvalue
cache 100;

CREATE SEQUENCE dataset_SEQ
start with 1
increment by 1
nomaxvalue
cache 100;

-- ============= Initializations

insert into job_status (id, status) values (1, 'New');
insert into job_status (id, status) values (2, 'Used');
insert into job_status (id, status) values (3, 'Success');
insert into job_status (id, status) values (4, 'Failure');
insert into job_status (id, status) values (5, 'Abandoned');

insert into trigger_segment_status (id, status) values (1, 'New');
insert into trigger_segment_status (id, status) values (2, 'Used');

insert into run_status (id, status) values (1, 'Active');
insert into run_status (id, status) values (2, 'CloseOut');
insert into run_status (id, status) values (3, 'CloseOutSegmentInjector');
insert into run_status (id, status) values (4, 'CloseOutScheduler');
insert into run_status (id, status) values (5, 'CloseOutRepack');
insert into run_status (id, status) values (6, 'CloseOutRepackMerge');
insert into run_status (id, status) values (7, 'CloseOutPromptReco');
insert into run_status (id, status) values (8, 'CloseOutRecoMerge');
insert into run_status (id, status) values (9, 'CloseOutDQMHarvest');
insert into run_status (id, status) values (10, 'CloseOutAlcaSkim');
insert into run_status (id, status) values (11, 'CloseOutAlcaSkimMerge');
insert into run_status (id, status) values (12, 'CloseOutExport');
insert into run_status (id, status) values (13, 'CloseOutT1Skimming');
insert into run_status (id, status) values (14, 'Complete');

insert into block_status (id, status) values (1, 'Active');
insert into block_status (id, status) values (2, 'Closed');
insert into block_status (id, status) values (3, 'InFlight');
insert into block_status (id, status) values (4, 'Exported');
insert into block_status (id, status) values (5, 'Skimmed');

insert into block_migrate_status (id, status) values (1, 'NotMigrated');
insert into block_migrate_status (id, status) values (2, 'Migrated');

insert into block_delete_status (id, status) values (0, 'ExpressNotDeletable');
insert into block_delete_status (id, status) values (1, 'NotDeletable');
insert into block_delete_status (id, status) values (2, 'Deletable');

insert into data_tier (id, name) values (1, 'RAW');
insert into data_tier (id, name) values (2, 'HLTDEBUG');
insert into data_tier (id, name) values (3, 'RECO');
insert into data_tier (id, name) values (4, 'ALCARECO');
insert into data_tier (id, name) values (5, 'AOD');
insert into data_tier (id, name) values (6, 'FEVT');
insert into data_tier (id, name) values (7, 'FEVTHLTALL');

insert into processing_style (id, name) values (1, 'Bulk');
insert into processing_style (id, name) values (2, 'Express');

commit;

