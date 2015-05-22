
cmssw = 'CMSSW_2_0_8'
process = 'FU'
streamToDatasets = {
  'A' : [ 'Cosmics' ],
  'DQM' : [ 'Debug' ],
  'Random' : [ 'RandomTriggers' ],
  'Calibration' : [ 'TestEnables' ]
}
datasetToTriggers = { 
  'Debug' : [ 'CosmicsPathDQM', 'RandomPathDQM', 'CalibrationPathDQM' ],
  'Cosmics' : [ 'CosmicsPath' ],
  'TestEnables' : [ 'CalibrationPath' ],
  'RandomTriggers' : [ 'RandomPath' ]
}

