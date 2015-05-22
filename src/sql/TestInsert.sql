




INSERT INTO run (run_id)VALUES (1), (2),(3),(4);

INSERT INTO lumi_section (lumi_section_id, run_id) VALUES 
  (1, 1), (2, 1), (3, 1), 
  (1, 2), (2, 2), (3, 2), 
  (1, 3), (2, 3), (3, 3), 
  (1, 4), (2, 4), (3, 4); 


INSERT INTO streamer (run_id, lumi_id, streamer_lfn) VALUES
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=1 and run_id=1), "run1_lumi1_file1.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=1 and run_id=1), "run1_lumi1_file2.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=1 and run_id=1), "run1_lumi1_file3.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=1 and run_id=1), "run1_lumi1_file4.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=1 and run_id=1), "run1_lumi1_file5.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=1 and run_id=1), "run1_lumi1_file6.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=2 and run_id=1), "run1_lumi2_file1.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=2 and run_id=1), "run1_lumi2_file2.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=2 and run_id=1), "run1_lumi2_file3.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=2 and run_id=1), "run1_lumi2_file4.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=2 and run_id=1), "run1_lumi2_file5.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=2 and run_id=1), "run1_lumi2_file6.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=3 and run_id=1), "run1_lumi3_file1.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=3 and run_id=1), "run1_lumi3_file2.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=3 and run_id=1), "run1_lumi3_file3.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=3 and run_id=1), "run1_lumi3_file4.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=3 and run_id=1), "run1_lumi3_file5.dat"),
  (1, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=3 and run_id=1), "run1_lumi3_file6.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=1 and run_id=2), "run2_lumi1_file1.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=1 and run_id=2), "run2_lumi1_file2.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=1 and run_id=2), "run2_lumi1_file3.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=1 and run_id=2), "run2_lumi1_file4.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=1 and run_id=2), "run2_lumi1_file5.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=1 and run_id=2), "run2_lumi1_file6.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=2 and run_id=2), "run2_lumi2_file1.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=2 and run_id=2), "run2_lumi2_file2.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=2 and run_id=2), "run2_lumi2_file3.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=2 and run_id=2), "run2_lumi2_file4.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=2 and run_id=2), "run2_lumi2_file5.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=2 and run_id=2), "run2_lumi2_file6.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=3 and run_id=2), "run2_lumi3_file1.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=3 and run_id=2), "run2_lumi3_file2.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=3 and run_id=2), "run2_lumi3_file3.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=3 and run_id=2), "run2_lumi3_file4.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=3 and run_id=2), "run2_lumi3_file5.dat"),
  (2, (SELECT lumi_id FROM lumi_section WHERE lumi_section_id=3 and run_id=2), "run2_lumi3_file6.dat");
