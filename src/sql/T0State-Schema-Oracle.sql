REM ======================================================================
REM ===   Sql Script for Database : Tier0-AST
REM ===
REM === Build : 42
REM ======================================================================

PROMPT run

CREATE TABLE run
  (
    run_id      int           not null,
    start_time  int,
    end_time    int,
    run_status  varchar(10),
    primary key(run_id)
  );

-- start_time Time Stamp Trigger

PROMPT TRrun_start
CREATE OR REPLACE TRIGGER TRrun_start BEFORE INSERT ON run
FOR EACH ROW declare
  unixtime integer
     :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
BEGIN
  if :NEW.start_time is null then :NEW.start_time := unixtime; end if;
END;
/

SHOW ERRORS;

-- end_time Time Stamp Trigger
PROMPT TRrun_end

CREATE OR REPLACE TRIGGER TRrun_end BEFORE UPDATE ON run
FOR EACH ROW declare
  unixtime integer
     :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
BEGIN
  if :NEW.end_time is null then :NEW.end_time := unixtime; end if;
END;
/
SHOW ERRORS;

REM ======================================================================
PROMPT lumi_section

CREATE TABLE lumi_section
 (
   lumi_id          int         not null,
   lumi_section_id  int         not null,
   run_id           int         not null,
   start_time       int,
   primary key(lumi_id),
   unique(lumi_section_id,run_id),
   foreign key(run_id) references run(run_id) on delete CASCADE
 );

create sequence seq_lumi;
PROMPT TRlumi_sec
CREATE OR REPLACE TRIGGER TRlumi_sec before insert on lumi_section
for each row begin
    if inserting then
      if :NEW.lumi_id is null then
         select seq_lumi.nextval into :NEW.lumi_id from dual;
      end if;
   end if;
end;
/

SHOW ERRORS;

-- start_time Time Stamp Trigger
PROMPT TRlumi_start
CREATE OR REPLACE TRIGGER TRlumi_start BEFORE INSERT ON lumi_section
FOR EACH ROW declare
  unixtime integer
     :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
BEGIN
  if :NEW.start_time is null then :NEW.start_time := unixtime; end if;
END;
/

SHOW ERRORS;


REM ======================================================================
PROMPT streamer
CREATE TABLE streamer
 (
   streamer_id  int            not null,
   lumi_id      int            not null,
   run_id       int            not null,
   start_time   int,
   filesize     int,
   events       int,
   lfn          varchar(255)   unique not null,
   primary key(streamer_id),
   foreign key(lumi_id) references lumi_section(lumi_id) on delete CASCADE,
   foreign key(run_id) references run(run_id) on delete CASCADE
 );

create sequence seq_streamer;

PROMPT TRstreamer
CREATE OR REPLACE TRIGGER TRstreamer before insert on streamer
for each row begin
    if inserting then
      if :NEW.streamer_id is null then
         select seq_streamer.nextval into :NEW.streamer_id from dual;
      end if;
   end if;
end;
/

SHOW ERRORS;

-- start_time Time Stamp Trigger
PROMPT TRstream_start
CREATE OR REPLACE TRIGGER TRstream_start BEFORE INSERT ON streamer
FOR EACH ROW declare
  unixtime integer
     :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
BEGIN
  if :NEW.start_time is null then :NEW.start_time := unixtime; end if;
END;
/

SHOW ERRORS;


REM ======================================================================
PROMPT trigger_details
CREATE TABLE trigger_details
 (
   trigger_id       int            not null,
   trigger_label    varchar(255)   unique not null,
   primary_dataset  varchar(255)   not null,
   algorithm     varchar2(10),    
   primary key(trigger_id),
   CONSTRAINT cons_algorithm  CHECK (algorithm IN ('accu', 'split'))
 );

create sequence seq_trig_details;
PROMPT TRtrig_detail
CREATE OR REPLACE TRIGGER TRtrig_detail before insert on trigger_details
for each row begin
    if inserting then
      if :NEW.trigger_id is null then
         select seq_trig_details.nextval into :NEW.trigger_id from dual;
      end if;
   end if;
end;
/

SHOW ERRORS;

REM ======================================================================
PROMPT repacked
CREATE TABLE repacked
  (
    repacked_id  int            not null,
    run_id       int            not null,
    filesize     int,
    events       int,
    lfn          varchar(500)   unique not null,
    primary key(repacked_id),
    foreign key(run_id) references run(run_id) on delete CASCADE
  );

create sequence seq_repacked;
PROMPT TRrepacked
CREATE OR REPLACE TRIGGER TRrepacked before insert on repacked
for each row begin
    if inserting then
      if :NEW.repacked_id is null then
         select seq_repacked.nextval into :NEW.repacked_id from dual;
      end if;
   end if;
end;
/

SHOW ERRORS;

REM ======================================================================
PROMPT trigger_segment
CREATE TABLE trigger_segment
 (
   segment_id    int              not null,
   streamer_id   int              not null,
   lumi_id       int              not null,
   run_id        int              not null,
   segment_size  int              not null,
   trigger_id    int              not null,
   trig_status         varchar2(50)   not null,
   primary key(segment_id),
   foreign key(streamer_id) references streamer(streamer_id) on delete CASCADE,
   foreign key(lumi_id) references lumi_section(lumi_id) on delete CASCADE,
   foreign key(run_id) references run(run_id) on delete CASCADE,
   foreign key(trigger_id) references trigger_details(trigger_id)
 );

create sequence seq_trig_seg;
PROMPT TRtrig_segment
CREATE OR REPLACE TRIGGER TRtrig_segment before insert on trigger_segment
for each row begin
    if inserting then
      if :NEW.segment_id is null then
         select seq_trig_seg.nextval into :NEW.segment_id from dual;
      end if;
   end if;
end;
/

SHOW ERRORS;

REM ======================================================================
PROMPT repack_seg_assoc
CREATE TABLE repack_seg_assoc
 (
   segment_id   int   not null,
   repacked_id  int   not null,
   foreign key(segment_id) references trigger_segment(segment_id),
   foreign key(repacked_id) references repacked(repacked_id)
 );

REM ======================================================================

-- JOB TABLES

REM =====================================================================
PROMPT merge_job
CREATE TABLE merge_job
  (
    job_id         int           not null,
    schedule_time  int,
    complete_time int,
    job_status         varchar(50)   not null,
    primary key(job_id)
  );

-- auto inc seq

create sequence seq_merge_jobid;
PROMPT seq_merge_jobid
CREATE OR REPLACE TRIGGER TRmerge_jobid before insert on merge_job
for each row begin
    if inserting then
      if :NEW.job_id is null then
         select seq_merge_jobid.nextval into :NEW.job_id from dual;
      end if;
   end if;
end;
/

SHOW ERRORS;

-- start_time Time Stamp Trigger
PROMPT TRmerge_start
CREATE OR REPLACE TRIGGER TRmerge_start BEFORE INSERT ON merge_job
FOR EACH ROW declare
  unixtime integer
     :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
BEGIN
  if :NEW.schedule_time is null then :NEW.schedule_time := unixtime; end if;
END;
/

SHOW ERRORS;
 
-- complete_time Time Stamp Trigger
PROMPT TRmerge_done
CREATE OR REPLACE TRIGGER TRmerge_done BEFORE UPDATE ON merge_job
FOR EACH ROW declare
  unixtime integer
     :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
BEGIN
  if :NEW.complete_time is null then :NEW.complete_time := unixtime; end if;
END;
/

SHOW ERRORS;

REM ======================================================================
PROMPT merge_job_repack_assoc
CREATE TABLE merge_job_repack_assoc
  (
    repack_id  int   not null,
    job_id     int   not null,
    foreign key(repack_id) references repacked(repacked_id) on delete CASCADE,
    foreign key(job_id) references merge_job(job_id) on delete CASCADE
  );

-- ============================================================
 
CREATE TABLE run_trig_assoc
 ( 
   trigger_id  int   not null,
   run_id      int   not null,
   foreign key(trigger_id) references trigger_details(trigger_id) on delete CASCADE,
   foreign key(run_id) references run(run_id) on delete CASCADE
 );


REM ======================================================================
REM ======================================================================
REM ======================================================================
PROMPT repack_job
CREATE TABLE repack_job
  (
    job_id         int           not null,
    job_type         varchar(50)   not null,
    schedule_time  int,
    complete_time int,
    job_status         varchar(50),
    primary key(job_id)
  );

-- auto inc seq

create sequence seq_repack_jobid;
PROMPT TRrepack_jobid
CREATE OR REPLACE TRIGGER TRrepack_jobid before insert on repack_job
for each row begin
    if inserting then
      if :NEW.job_id is null then
         select seq_repack_jobid.nextval into :NEW.job_id from dual;
      end if;
   end if;
end;
/

SHOW ERRORS;

-- start_time Time Stamp Trigger
PROMPT TRrepack_start
CREATE OR REPLACE TRIGGER TRrepack_start BEFORE INSERT ON repack_job
FOR EACH ROW declare
  unixtime integer
     :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
BEGIN
  if :NEW.schedule_time is null then :NEW.schedule_time := unixtime; end if;
END;
/

SHOW ERRORS;

-- complete_time Time Stamp Trigger
PROMPT TRrepack_done
CREATE OR REPLACE TRIGGER TRrepack_done BEFORE UPDATE ON repack_job
FOR EACH ROW declare
  unixtime integer
     :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
BEGIN
  if :NEW.complete_time is null then :NEW.complete_time := unixtime; end if;
END;
/

SHOW ERRORS;

REM ======================================================================

PROMPT repack_job_streamer_assoc
CREATE TABLE repack_job_streamer_assoc
  (
    streamer_id  int   not null,
    job_id       int   not null,
    foreign key(streamer_id) references streamer(streamer_id) on delete CASCADE,
    foreign key(job_id) references repack_job(job_id) on delete CASCADE
  );

PROMPT repack_job_trig_seg_assoc

CREATE TABLE repack_job_trig_seg_assoc
  (
    job_id      int   not null,
    segment_id  int   not null,
    foreign key(job_id) references repack_job(job_id),
    foreign key(segment_id) references trigger_segment(segment_id)
  );

REM ======================================================================


commit;

