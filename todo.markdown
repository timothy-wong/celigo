### SMALL FIXES
- [x] get_table() exit process after finishing
- [x] read() should accept the backslash character
- [ ] add console.log messages (to everything really)

### MEDIUM FIXES
- [ ] add clean up to get_training() if errors out
    - [ ] remove td_temp in /var/lib/docker/volumes/CABINET/_data/DataPipeline

### BIG FIXES
- [ ] T2 Clustering
    - [ ] Test clustering methods
        - [ ] Spacy clustering post training
            - [ ] Essence
            - [ ] Message
        - [ ] Edit distance
        - [ ] App/Code
    - [ ] t2_clusters.json should be written into CABINET/DataPipeline/DataProcesserBackups
    - [ ] t2_clusters.json used in label()
- [x] sort_precluster() in read() is SLOW (currently pulling out alll the hashes in master_precluster for comparison)
    - [x] Make hash an index into master_precluster // (already a primary key which is the same as a unique index)
    - [x] async.map query master_precluster for every new error to find if exists
- [x] get_training() should ignore null and 'none' valued errors in master
    - [x] combine null + 'none' valued errors in master by not updating none values in label()
    - [x] get_training() doesn't select null values
- [ ] implement mysql pool for parallel querying
- [ ] write helptext lmao


### REALLY BIG FIXES
- [ ] Make modules 2 and 3 atomic
    - [ ] Copy all existing tables
    - [ ] Run existing module
    - [ ] If error drop copies
    - [ ] Else drop originals and rename copies
    - [ ] Check that module restart works
- [ ] Add batch ids to master
    - [ ] get_training() accepts a --batch flag to output specific batches
    - [ ] read() logs errors, files, per batch
    - [ ] new log() function that will print batch information
- [ ] add clean up to every function
    - [ ] for every main function the error passes along which stage the function was at when it errored so the function can clean up properly
- [ ] A,,,node container,,,,perhaps,,,,

---------------------------------------------------------------------------------------------


### TEST
- [ ] read()
    - [ ] module 1
        - [ ] check uuid_join values
    - [ ] module 2
    - [ ] module 3
    - [ ] module 4
    - [ ] atomicity
- [x] label()
- [x] get_training()
- [x] get_table()
- [ ] dump()
- [ ] log()
- [ ] help()
