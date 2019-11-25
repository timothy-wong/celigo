SMALL FIXES
  [] get_table() exit process after finishing
  [] read() should accept the backslash character

BIG FIXES
  [] T2 Clustering
    [] Test clustering methods
      [] Spacy clustering post training
        [] Essence
        [] Message
      [] Edit distance
      [] App/Code
    [] t2_clusters.json should be written into CABINET/DataPipeline/DataProcesserBackups
    [] t2_clusters.json used in label()
  [] sort_precluster() in read() is SLOW (currently pulling out alll the hashes in master_precluster for comparison)
    [] Make hash an index into master_precluster
    [] async.map query master_precluster for every new error to find if exists
  [] get_training() should ignore null and 'none' valued errors in master
    [] combine null + 'none' valued errors in master by not updating none values in label()
  
REALLY BIG FIXES
  [] Make modules 2 and 3 atomic
    [] Copy all existing tables
    [] Run module
    [] If error drop copies
    [] Else drop originals and rename copies
    [] Check that module restart works
  [] Add batch ids to master
    [] get_training() accepts a --batch flag to output specific batches
    [] read() logs errors, files, per batch
    [] new log() function that will print batch information
