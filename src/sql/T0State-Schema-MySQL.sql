

DROP DATABASE IF EXISTS T0AST;
CREATE DATABASE T0AST;
USE T0AST;

/* ======================================================================
 * General Repacker tables
 *
 */

CREATE TABLE run (
 /*
  * run:  Every file belongs to a run
  *       This is the prime entity in the DB, everything should
  *       be traced by run number
  * Fields:
  * - run_id The run number
  * - start_time   The time the run entered the system
  */ 
  run_id INT(11) NOT NULL,
  start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (run_id),
  PRIMARY KEY (run_id)
	  
) TYPE=InnoDB;


CREATE TABLE run_finished (
   run_id INT(11) NOT NULL,
   end_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
   FOREIGN KEY(run_id) REFERENCES run(run_id) 
       ON DELETE CASCADE

) TYPE=InnoDB;

CREATE TABLE lumi_section (
 /*
  * lumi_section: All streamers belong to a lumi section
  *               All Lumi sections belong to a run
  * Fields:
  * - lumi_id   Internal index used to provide cross links to other entities
  * - lumi_section_id  Actual Lumi Seg ID within run
  * - run_id           Run owning lumi section
  * - start_time       Time lumi section entered the system
  */
  lumi_id INT(11) NOT NULL AUTO_INCREMENT,
  lumi_section_id INT(11) NOT NULL,
  run_id INT(11) NOT NULL,
  start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
  PRIMARY KEY(lumi_id),
  UNIQUE (run_id, lumi_section_id),
  FOREIGN KEY(run_id) REFERENCES run(run_id) 
       ON DELETE CASCADE

) TYPE=InnoDB;



CREATE TABLE trigger_details (
 /*
  * trigger: Named map/mask for a given trigger.
  * 
  * Fields:
  * trigger_id   Internal index used to refer to a trigger
  * trigger_label Real world trigger name
  */
  trigger_id INT(11) NOT NULL AUTO_INCREMENT,
  trigger_label VARCHAR(255) NOT NULL,
  primary_dataset VARCHAR(255) NOT NULL,	  
  UNIQUE(trigger_label ),
  PRIMARY KEY(trigger_id)


) TYPE=InnoDB;


CREATE TABLE streamer (
  /*
   * streamer:  Entry describing a streamer file
   *            All streamers belong to a run/lumi pair
   * Fields:
   * - streamer_id  Internal index used to link streamer to other entities
   * - lumi_id      Reference to lumi section owning streamer
   * - run_id       Reference to run owning streamer
   * - start_time   Time of entry of streamer into system
   * 
   */
   streamer_id INT(11) NOT NULL AUTO_INCREMENT,
   lumi_id INT(11) NOT NULL,
   run_id INT(11) NOT NULL,
   start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,

   FOREIGN KEY(run_id) REFERENCES run(run_id) ON DELETE CASCADE,
   FOREIGN KEY(lumi_id) REFERENCES lumi_section(lumi_id) ON DELETE CASCADE,
   PRIMARY KEY(streamer_id)

) TYPE=InnoDB;



CREATE TABLE streamer_details (
  /*
   * streamer_details : data on a streamer file that is used to
   * create jobs, but otherwise is factored out of most queries
   * 
   */
   streamer_id INT(11) NOT NULL,
   lfn VARCHAR(255),
   pfn VARCHAR(255),
   filesize INT(11),
   events INT(11),
   lumi_id INT(11) NOT NULL,
   run_id INT(11) NOT NULL,
 
   FOREIGN KEY(run_id) REFERENCES run(run_id),
   FOREIGN KEY(lumi_id) REFERENCES lumi_section(lumi_id), 
   FOREIGN KEY(streamer_id) REFERENCES streamer(streamer_id)
        ON DELETE CASCADE,
   PRIMARY KEY (lfn)

) TYPE=InnoDB;

CREATE TABLE trigger_segment (
  /* 
   * trigger_segment: Fraction of a streamer file corresponding to some
   *                  trigger tag.
   *
   * Fields:
   * - segment_id   Internal index used to connect segment with other entities
   * - streamer_id  Index of parent streamer file
   * - lumi_id      Reference to lumi section owning parent streamer
   * - run_id       Reference to run owning parent streamer
   * - algorithm    Name of processing algorithm (defined by trigger/dataset)
   */
  	
   segment_id INT(11) NOT NULL AUTO_INCREMENT,
  
   streamer_id INT(11) NOT NULL,
   lumi_id INT(11) NOT NULL,
   run_id INT(11) NOT NULL,
   trigger_id INT(11) NOT NULL,
   segment_size INT(11) NOT NULL,
   algorithm ENUM('accumulator', 'split'),

   FOREIGN KEY(run_id) REFERENCES run(run_id) ON DELETE CASCADE,
   FOREIGN KEY(lumi_id) REFERENCES lumi_section(lumi_id) ON DELETE CASCADE, 
   FOREIGN KEY(streamer_id) REFERENCES streamer(streamer_id) ON DELETE CASCADE,
   FOREIGN KEY(trigger_id) REFERENCES trigger_details(trigger_id),
   PRIMARY KEY(segment_id)


) TYPE=InnoDB;


CREATE TABLE repacked (
 /*
  * repacked : Entity representing a repacked file output and links 
  *            to its constituent trigger segments
  * Fields:
  * - repacked_id   Internal index used to connect with other entities
  * - run_id        Run number owning the repacked file
  */
  repacked_id INT(11) NOT NULL AUTO_INCREMENT,
  run_id INT(11) NOT NULL,
  PRIMARY KEY(repacked_id),
  FOREIGN KEY(run_id) REFERENCES run(run_id) ON DELETE CASCADE


) TYPE=InnoDB;


CREATE TABLE repack_seg_assoc (
  /*
   * repack_seg_assoc : Link between repacked file and trigger segment
   * 
   * Fields:
   * - segment_id  Index of segment contained in repack file
   * - repacked_id Index of repacked file containing segment
   */
   segment_id INT(11) NOT NULL,
   repacked_id INT(11) NOT NULL,
  
   FOREIGN KEY(repacked_id) REFERENCES repacked(repacked_id),
   FOREIGN KEY(segment_id) REFERENCES trigger_segment(segment_id)

) TYPE=InnoDB;

/* End General Repacker Entities
 * =============================================================
 */


/* =============================================================
 * Accumulator model specific tracking
 * Here we track trigger segments that are scheduled for 
 * accumulator jobs, successful accumulated segments and
 * failed segments
 */
CREATE TABLE accum_job_scheduled (
 /*
  * Table containing definition of a repacker job for a set of
  * trigger segments
  */
  accum_job_id INT(11) NOT NULL AUTO_INCREMENT,
  schedule_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(accum_job_id)
) TYPE=InnoDB;

CREATE TABLE accum_job_seg_assoc (
 /*
  * Associate a trigger segment to a scheduled accumulator job
  *
  */
  segment_id INT(11) NOT NULL,
  accum_job_id INT(11) NOT NULL,  
  FOREIGN KEY(segment_id) REFERENCES trigger_segment(segment_id),
  FOREIGN KEY(accum_job_id) REFERENCES accum_job_scheduled(accum_job_id)

)TYPE=InnoDB;


CREATE TABLE accum_job_complete (
 /*
  * Status of an accumulator job is completed/successful
  *
  */
  accum_job_id INT(11) NOT NULL AUTO_INCREMENT,
  complete_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
  FOREIGN KEY(accum_job_id) REFERENCES accum_job_scheduled(accum_job_id)  
 
) TYPE=InnoDB;


CREATE TABLE accum_job_failed ( 
 /*
  * Status of an accumulator job is failed
  *
  */
  accum_job_id INT(11) NOT NULL AUTO_INCREMENT,
  failed_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
  FOREIGN KEY(accum_job_id) REFERENCES accum_job_scheduled(accum_job_id)  
) TYPE=InnoDB;

/* End Accumulator pieces
 * ==================================================================
 */

/* ==================================================================
 *
 * Split/Merge specific algorithm tracking
 * We map trigger segments to split jobs that will split them out
 * of the streamer file and schedule them for merging to make a repacked
 * file.
 */
CREATE TABLE split_trigger_segment (
  /*
   * split_trigger_segment
   * Status of a segment that has been split out of a segment
   * by a split job and is awaiting merging
   */
   segment_id INT(11) NOT NULL,
   FOREIGN KEY(segment_id) REFERENCES trigger_segment(segment_id)
) TYPE=InnoDB;

CREATE TABLE merged_trigger_segment (
 /*
  * merged_trigger_segment
  * Status of a segment that has been split to a seperate file and
  * then merged
  */
  segment_id INT(11) NOT NULL,
  FOREIGN KEY(segment_id) REFERENCES trigger_segment(segment_id)
) TYPE=InnoDB;


CREATE TABLE split_job_scheduled (
  /*
   * Entity representing a split job
   *
   */
  split_job_id INT(11) NOT NULL AUTO_INCREMENT,
  schedule_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
  PRIMARY KEY(split_job_id)

) TYPE=InnoDB;

CREATE TABLE split_job_seg_assoc (
  /* 
   * Association between a trigger segment and the split job
   * scheduled to split it
   *
   */
   segment_id INT(11) NOT NULL,
   split_job_id INT(11) NOT NULL,  
   FOREIGN KEY(segment_id) REFERENCES trigger_segment(segment_id),
   FOREIGN KEY(split_job_id) REFERENCES split_job_scheduled(split_job_id)

) TYPE=InnoDB;

/* End Split/Merge algo pieces
 * =============================================================
 */
