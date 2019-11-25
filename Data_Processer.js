const async = require('async')
const crypto = require('crypto')
const csv_parser = require('csv-parse')
const csv_writer = require('csv-writer').createObjectCsvWriter
const csv_writer_arr = require('csv-writer').createArrayCsvWriter
const fs = require('fs')
const mysql = require('mysql')
const request = require('request')
const readline = require('readline')

// FIXED VARIABLEs --------------------------------------------------------------------------------------------------------------------------

const batch_size = 50
const chunk_size = 10000
const count_chunk_size = 250
const join_size  = 100
const relevant_fields = ['application', 'code', 'message']
const master_cols = ['application', 'code', 'essence']


const master_table = 'master'
const cluster_map = 'cluster_map'
const run_map = 'run_map'
const master_precluster_table = 'master_precluster'
const userid_table = 'userid_table'
const io_table = 'errors_io'

const io_example = {'application': '.', 'agentId' : '', '_ID': '.'}
const error_example = {'type': '.', 'time': 0, 'timeTaken': 0, '_userId': '.', '_connectorId': '.', '_integrationId': '.', '_flowId': '.', '_flowJobId': '.', '_exportId': '.', '_importId': '', 'oIndex': 0, '_connectionId': '.', 'isDataLoader': '.', 'traceKey': '.', 'retryDataKey': '.', 'exportDataURI': '.', 'importDataURI': '.', 'exportField': '.', 'importField': '.', 'source': '.', 'code': '.', 'message': '.'}
const userid_example = {'application': '.', 'code': '.', 'message': '.', 'hash': '.', 'userid': '.', 'count': 0}
const run_example = {'essence': '.', 'hash': '.'}

const helptext = 
`
PLACEHOLDER
`
const alphanumeric_regex = new RegExp('\\w+')

// HELPER FUNCTIONS -------------------------------------------------------------------------------------------------------------------------


/* 
Takes in milliseconds MS and returns it as a string formatted into minutes, seconds and milliseconds.
*/
function time_string(ms) {
  let secs = Math.floor(ms/ 1000)
  ms = ms - (secs * 1000)
  let mins = Math.floor(secs / 60)
  secs = secs - (mins * 60)
  return mins.toString() + ' m ' + secs.toString() + ' s ' + ms.toString() + ' ms' 
}

/*
Takes in an array ARR = [a, b, ..., c] and non-destructively returns a string '(a, b, ..., c)'. 
Array elements are assumed to be able to be concatenated to a string.
*/
function bracket(arr) {
  let first = arr[0]
  let bracketed = '(' + first
  arr.shift()
  for (e of arr) {
    bracketed = bracketed.concat(', ')
    bracketed = bracketed.concat(e)
  }
  bracketed = bracketed.concat(')')
  arr.unshift(first)
  return bracketed
}

/*
Takes in an array ARR = [a, b, ..., c] and non-destructively returns a string 'a, b, ..., c'. 
Array elements are assumed to be able to be concatenated to a string.
*/
function comma(arr) {
  return arr.join(', ')
}

/*
Takes in HEADERS and VALUES and returns a json object with headers as keys mapping to values.
Assumes that values is at least as long as headers and that values are ordered as needed for the mapping.
*/
function make_obj(headers, values) {
  let objects = []
  for (v of values) {
    let obj = []
    for (let i = 0; i < headers.length; i ++) {
      obj[headers[i]] = v[i]
    }
    objects.push(obj)
  }
  return objects
}

/*
Connect to database. 
Requires a CALLBACK to return the connection.
*/
function connect_db(callback) {
  const connection = mysql.createConnection({
    host     : 'localhost',
    port     : '3306',
    database : 'LBLR_data',
    user     : 'root',
    password : 'raceboorishabackignitedrum',
  })

  connection.connect(function(err) {
    if (err) {
      callback(err)
    } else {
      callback(null, connection)
    }
  })
}

/*
Creates a table with NAME and columns given by EXAMPLE_DATA using CONNECTION. 
Accepts a CALLBACK to pass on errors, otherwise returns nothing. 
*/
function create_table(name, example_data, connection, callback) {
  let create_query = 'CREATE TABLE ' + name + ' '
  const keys = Object.keys(example_data)
  let cols = []

  for (k of keys) {
    let col_string = k
    if (typeof example_data[k] === 'number') {
      col_string = col_string.concat(' BIGINT ')
    } else {
      col_string = col_string.concat(' VARCHAR(500) ')
    }
    cols.push(col_string)
  }
  create_query = create_query.concat(bracket(cols))

  connection.query(create_query, callback)
}

/*
Drops table with NAME using CONNECTION.
Accepts a CALLBACK to pass on errors, otherwise returns nothing. 
*/
function drop_table(name, connection, callback) {
  const drop_query = 'DROP TABLE ' + name

  connection.query(drop_query, (err, result) => {
    if (err) {
      callback(err)
    } else {
      callback(null)
    }
  })
}

/* 
Read in a csv file at FILEPATH. Requires a CALLBACK to pass on the data.
Returns the file data through the callback as OUTPUT. 
File data is formatted as an array of lines of file data.
*/
function read_csvfile(filepath, callback) {
  const output = []
  try {
    fs.createReadStream(filepath)
    .pipe(csv_parser({delimiter: ':'}))
    .on('data', function(row) {
      output.push(row[0].split(','))   
    })
    .on('end',function() {
      callback(null, output)
    })
  } catch (err) {
    callback(err)
  }
}

/*
Read in multiple csv files containing data split across files (assumes that the data is formatted the same across files).
Files are given by an array of strings FILEPATHS. Requires a CALLBACK to pass on the data.
Returns the file data through the callback as OUTPUT. Returns null if no filepaths passed in.
*/
function read_csvfiles(filepaths, callback) {
  async.map(filepaths, 
  (fp, map_callback) => {
    read_csvfile(fp, (err, output) => {
      if (err) {
        map_callback(err)
      } else {
        map_callback(null, output)
      }
    })
  },
  (err, result) => {
    if (err) {
      callback(err)
    } else {
      let output = result[0]
      result.shift()
      for (r of result) {
        r.shift()
        output = output.concat(r)
      }
      callback(null, output)
    }
  })
}

/* 
Read in a json file at FILEPATH. Requires a CALLBACK to pass on the data.
Returns the file data through the callback as OUTPUT. 
File data is formatted as an array of lines of file data.
*/
function read_jsonfile(filepath, callback) {
  fs.readFile(filepath, (err, file_data) => {
    if (err) {
      callback(err)
    } else {
      let output 
      try {
        output = JSON.parse(file_data)
        console.log('Read in json file: ' + filepath)
      } catch (err) {
        callback(err)
      }
      callback(null, filepath, output)
    }
  })
}

/* 
Insert an array of OBJECTS into TABLE using CONNECTION. Assumes objects are correctly formatted for insert.
Accepts a CALLBACK to pass on errors, otherwise returns nothing.
*/
function insert_table(objects, table, connection, ignore=false, callback) {
  if (objects && objects.length) {

    const keys = Object.keys(objects[0])
    const cols = bracket(keys)
    let insert_query
    if (ignore) {
      insert_query = 'INSERT IGNORE INTO ' + table + ' ' + cols + ' VALUES ?'
    } else {
      insert_query = 'INSERT INTO ' + table + ' ' + cols + ' VALUES ?'
    }

    if (objects.length > chunk_size) {
      const to_insert = []
      for (object of objects.slice(0, 10000)) { 
        const curr_values = []
        for (k of keys) {
          curr_values.push(object[k])
        }
        to_insert.push(curr_values)
      }

      connection.query(insert_query, [to_insert], (err, ret, fields) => {
        if (err) { 
          callback(err)
        } else {
          insert_table(objects.slice(10000), table, connection, ignore, callback)
        }
      })
    } else {
      const to_insert = []
      for (object of objects) { 
        const curr_values = []
        for (k of keys) {
          curr_values.push(object[k])
        }
        to_insert.push(curr_values)
      }

      connection.query(insert_query, [to_insert], (err, ret, fields) => {
        if (err) {
          callback(err)
        } else {
          callback(null)
        }
      })
    }

  } else {
    callback(null)
  }
}

/*
Read in multiple error json files containing data split across files (assumes that the data is formatted the same across files).
Files are given by an array of strings FILEPATHS. Inserts all of the entries into TABLE using CONNECTION.
Requires a CALLBACK to pass on the number of errors inserted.
*/
function insert_errorfiles(filepaths, table, connection, callback) {
  if (filepaths && filepaths.length) {
    async.waterfall([
      // Read in files
      function read_json(wcb) {
        async.map(filepaths, 
        (fp, map_callback) => {
          read_jsonfile(fp, (err, filepath, output) => {
            if (err) {
              map_callback(err)
            } else {
              map_callback(null, output)
            }
          })
        },
        (err, result) => {
          if (err) {
            wcb(err)
          } else {
            let output = result[0]
            result.shift()
            for (r of result) {
              output = output.concat(r)
            }
            console.log('Finished reading in ' + filepaths.length + ' files.')
            wcb(null, output)
          }
        })
      },
      // Insert files
      function insert_json(output, wcb) {
        const well_formed = []
        for (error of output) {
          if (error['message'].length < 500) {
            error['message'] = get_list_of_strings(error['message'])
            well_formed.push(error)
          }
        }
        const keys = Object.keys(well_formed[0])
        for (e of well_formed) {
          for (k of keys) {
            if (e[k] && e[k].length > 500) {
              e[k] = e[k].slice(0, 500)
            }
          }
        }
        insert_table(well_formed, table, connection, null, (err) => {
          if (err) {
            wcb(err)
          } else {
            wcb(null, output.length)
          }
        })
      }
    ], 
    function (err, num) {
      if (err) {
        callback(err)
      } else {
        callback(null, num)
      }
    })
  } else {
    callback(null, 0)
  }
}

/*
Takes in a string STR and removes all non-alphanumeric characters while preserving a one space distance between words.
*/
function clean(str) {
  let ret = JSON.stringify(str).replace(/[^0-9a-z_']/gi, ' ')
  ret = ret.replace(/(^|\s+)(\d+(\s+|$))+/g, ' ')
  ret = ret.replace(/(^|\s+)(\S(\s+|$))+/g, ' ')
  ret = ret.replace(/\s\s+/g, ' ')

  ret = ret.toLowerCase()
  if (ret.slice(0, 1) === ' ') {
    ret = ret.slice(1)
  }
  if (ret.slice(-1) === ' ') {
    ret = ret.slice(0, -1)
  }

  return ret
}

/*
Takes in an unformatted message and return a well formed error message
*/

function get_list_of_strings(jsonFile) {
  var retList = ''

  try {
      // If it does not parse for us
      // let jsonContent = JSON.parse(JSON.parse(jsonFile));

      // If it does parse for us
      let jsonContent = JSON.parse(jsonFile);
      if (jsonContent && typeof jsonContent === "object") {
          for (const key of Object.keys(jsonContent)) {
              const objValue = jsonContent[key]
              if (!_.isObject(objValue)) {
                  retList += getListOfStrings(jsonContent[key])
              }
              else {
                  retList += getListOfStrings(jsonContent[key])
              }
              
          }
          return clean(retList)
      }

      throw new Error('not JSON')
      
  } catch (e) {
      var objType = typeof jsonFile
      if (objType === 'string') {
          return clean(jsonFile) + ' '
      }
      else if (objType === 'number') {
          //numbers do not add any value so no need to add them
          return ''
      }
      else if (Array.isArray(jsonFile)) {
          var i;
          for (i = 0; i < jsonFile.length; i++) {
              retList += getListOfStrings(jsonFile[i])
          }
          return retList
      }
      else if (objType === 'object') {
          for (const key of Object.keys(jsonFile)) {
              retList += getListOfStrings(jsonFile[key])
          }
          return retList
      }
  }
  return retList
}

/*
Takes in a list of ERRORS and returns an array of clusters formed by essence matching.
*/
function t1_clustering(errors) {
  const clusters = []
  for (error of errors) {
    if (clusters.length == 0) {
      const first = []
      first.push(error)
      clusters.push(first)
    } else {
      let in_cluster = false
      for (cluster of clusters) {
        const prototype = cluster[0]
        if (prototype['essence'] === error['essence']) {
          cluster.push(error)
          in_cluster = true
          break
        }
      }
      if (!in_cluster) {
        const new_cluster = []
        new_cluster.push(error)
        clusters.push(new_cluster)
      }
    }
  }
  return clusters
}

/*
Processes a batch of json error files given by JSON_FPATHS containing ~250,000 errors.
Inserts the errors into UUID TABLE with processing ID using CONNECTION.
Hardcoded examples of error and io data.
Accepts a CALLBACK to pass on errors, otherwise returns nothing.
*/ 
function batch(filepaths, userid_table, id, connection, callback) {d
  const json_table = 'errors_json' + '_' + id.toString()
  console.log('PROCESSING BATCH ' + id)
    
  async.waterfall([
    // Create tables
    function create_json(cb) {
      create_table(json_table, error_example, connection, (err, result) => {
        if (err) {
          console.log('BATCH ' + id + ': Failed to create table: ' + json_table)
          cb(err) 
        } else {
          console.log('BATCH ' + id + ': Finished creating table: ' + json_table)
          cb(null)
        }
      })
    },
    // Insert files
    function insert_json(cb) {
      insert_errorfiles(filepaths, json_table, connection, (err, num) => {
        if (err) {
          console.log('BATCH ' + id + ': Failed to insert files')
          cb(err)
        } else {
          console.log('BATCH ' + id + ': Inserted ' + num + ' new errors into ' + json_table + '.')
          cb(null)
        }
      })
    },
    // Join json and csv tables. Returns errors with keys app, code, message, userid, hash
    function join_files(cb) {
      let join_query = 'SELECT '
      const cols = []
      const groupby_cols = []
      for (rf of relevant_fields) {
        cols.push(json_table + '.' + rf + ' AS ' + rf)
        groupby_cols.push(rf)
      }
      cols.shift()
      cols.push(io_table + '.application AS application')
      cols.push(json_table + '._userId AS userid')
      groupby_cols.push('userid')
      const cols_string = comma(cols)
      const groupby_string = comma(groupby_cols)

      join_query = join_query.concat(cols_string + ', COUNT(*) AS count FROM ' + json_table + ' JOIN ' + io_table + ' ON ' + json_table + '._connectionId = ' + io_table + '._ID GROUP BY ' + groupby_string)

      connection.query(join_query, function(err, result) {
        if (err) {
          console.log('BATCH ' + id + ': Error joining tables.')
          cb(err)
        } else {
          for (error of result) {
            let field_hashes = []
            for (field of relevant_fields) {
              let hash = crypto.createHash('sha256') 
              if (error[field]) {
                field_hashes.push(hash.update(error[field]).digest('hex'))
              } else {
                field_hashes.push(hash.update('').digest('hex'))
              }
            }
            let final_hash = crypto.createHash('sha256')
            merged_str = field_hashes.join('')
            let hash_string = final_hash.update(merged_str).digest('hex')
            error['hash'] = hash_string
          }
          console.log('BATCH ' + id + ': Finished joining tables: joined ' + result.length + ' rows.')
          cb(null, result)
        }
      })
    },
    // Insert into userid_table
    function insert_uuid(userid_data, cb) {
      console.log('BATCH ' + id + ': Started insert into: ' + userid_table)
      if (userid_data && userid_data.length) {
        insert_table(userid_data, userid_table, connection, null, (err) => {
          if (err) {
            console.log('BATCH ' + id + ': Error inserting new errors into ' + userid_table + '.')
            cb(err)
          } else {
            console.log('BATCH ' + id + ': Finished inserting new errors into ' + userid_table + '.')
            cb(null, connection)
          }
        })
      } else {
        console.log('BATCH ' + id + ': Finished inserting new errors into ' + userid_table + '.')
        cb(null, connection)
      }
    },
    // Flush json table
    function drop_json(connection, cb) {
      drop_table(json_table, connection, (err) => {
        if (err) {
          console.log('BATCH ' + id + ': Error dropping table ' + json_table)
          cb(err)
        } else {
          console.log('BATCH ' + id + ': Dropped table ' + json_table )
          cb(null)
        }
      })
    }
  ], 
  function (err, result) {
    if (err) {
      callback(err)
    } else {
      console.log('FINISHED PROCESSING BATCH ' + id)
      callback(null)
    }
  })
}

/*
Recursively processes BATCHES to insert errors into UUID_TABLE with COUNT as a batch processing ID using CONNECTION.
Accepts a callback to pass on errors, otherwise returns nothing.
*/
function batch_rec(batches, userid_table, count, connection, callback) {
  if (batches && batches.length) {
    batch(batches[0], userid_table, count, connection, (err) => {
      if (err) {
        callback(err)
      } else {
        batches.shift()
        count ++
        return batch_rec(batches, userid_table, count, connection, callback)
      }
    })
  } else {
    callback(null)
  }
}

/*
Processes BATCHES to insert errors into UUID_TABLE with COUNT as a batch processing ID using CONNECTION.
Aggregates the errors in  UUID_TABLE.
Requires a callback to pass on errors.
*/
function batch_sync(batches, userid_table, connection, callback) {
  async.waterfall([
    // Process the files and insert into userid_table
    function process(cb) {
      batch_rec(batches, userid_table, 0, connection, (err) => {
        if (err) {
          callback(err)
        } else {
          callback(null)
        }
      })
    },
    // Aggregate errors from userid_table
    function select(cb) {
      let cols = []
      for (rf of relevant_fields) {
        cols.push(rf)
      }
      cols.push('hash')
      cols.push('userid')
      const cols_string = comma(cols)
      const select_query = "SELECT *, SUM(count) AS count FROM " + userid_table + " GROUP BY " + cols_string
      
      connection.query(select_query, (err, result, fields) => {
        if (err) {
          cb(err)
        } else {
          cb(null, result)
        }
      })
    },
    // Flush userid_table
    function del(output, cb) {
      const delete_query = "DELETE FROM " + userid_table
      connection.query(delete_query, (err, result) => {
        if (err) {
          console.log('Error flushing: ' + userid_table)
          cb(err)
        } else {
          console.log('Flushed: ' + userid_table)
          cb(null, output)
        }
      })
    },
    // Insert aggregated errors into userid_table
    function insert(output, cb) {
      insert_table(output, userid_table, connection, (err) => {
        if (err) {
          console.log('Error aggregating errors in: ' + userid_table)
          cb(err)
        } else {
          console.log('Aggregated ' + output.length + ' errors into: ' + userid_table)
          cb(null, output)
        }
      })
    },
  ], 
  function (err, result) {
    if (err) {
      callback(err)
    } else {
      console.log(2)
      console.log(result.length)
      callback(null, result)
    }
  })
}

/*
Takes in MESSAGES and queries the flask server to return ESSENCES.
Requires a CALLBACK to pass on the data.
*/
function flask_convert(messages, callback) {
  request({
    method: 'PUT',
    url: 'http://localhost:5000',
    json: messages
  }, 
  function (err, request, body) {
    if (err) {
      callback(err)
    } else {
      callback(null, body)
    }
  })
}

/*
Takes in ERRORS and queries the flask server to return a list of cluster indices.
Requires a CALLBACK to pass on the data.
<-------------------------------------------------------------------------------------------------------------------- FIX
*/
function t2_convert(errors, callback) {
  const t2_clusters = []
  for (e of errors) {
    let cluster = []
    cluster.push(e)
    t2_clusters.push(cluster)
  }
  callback(null, t2_clusters)
}

/*
Returns a deep copy of OBJ
*/
function clone(obj) {
  return JSON.parse(JSON.stringify(obj))
}

/*
Makes a dictionary mapping hashes to their associated cluster value using T2 CLUSTERS.
Requires a CALLBACK to pass on the dictionary.
*/
function mk_hash_dict(t2_clusters, connection, callback) {
  const essence_dict = {}
  let count = 0
  for (cluster of t2_clusters) {
    for (error of cluster) {
      essence_dict[error['essence']] = count
    }
    count ++
  }

  const select_map = 'SELECT * FROM ' + run_map
  connection.query(select_map, (err, result, field) => {
    if (err) {
      callback(err)
    } else {
      const hash_dict = {}
      for (r of result) {
        hash_dict[r['hash']] = essence_dict[r['essence']]
      }
      callback(null, hash_dict)
    }
  })
 }

/*
Returns a list of prototypes corresponding to each cluster in T2 CLUSTERS. Passes on CONNECTION to get_prototypes.
Requires a CALLBACK to pass on PROTOTYPES.
*/
function get_prototypes(t2_clusters, connection, callback) {
  mk_hash_dict(t2_clusters, connection, (err, hash_dict) => {
    if (err) {
      callback(err)
    } else {
      const select_query = 'SELECT * FROM ' + userid_table;
      connection.query(select_query, (err, result, fields) => {
        if (err) {
          callback(err)
        } else {
          const cluster_data = {}
          for (let i = 0; i < t2_clusters.length; i ++) {
            let base = []
            let cluster_userids = new Set()
            base.push(cluster_userids)
            base.push(0)
            cluster_data[i] = base
          }

          for (r of result) {
            let r_hash = r['hash']
            if (r_hash in hash_dict) {
              let r_cluster = hash_dict[r_hash]
              let curr_data = cluster_data[r_cluster]
              curr_data[0].add(r['userid'])
              curr_data[1] += r['count']
              cluster_data[r_cluster] = curr_data
            }
          }
          const prototypes = []
          for (let i = 0; i < t2_clusters.length; i ++) {
            let prototype = clone(t2_clusters[i][0])
            delete prototype['essence']
            delete prototype['hash']
            let data = cluster_data[i]
            prototype['userids'] = data[0].size
            prototype['count'] = data[1]
            prototypes.push(prototype)
          }
          callback(null, prototypes)
        }
      })
    }
  })
}

/*
Recovers Tier 2 clusters in Module 4 setup at FILEPATH. 
Requires a callback to return the clusters.
*/
function recover_clusters(filepath, callback) {
  read_jsonfile(filepath, (err, fp, output) => {
    if (err) {
      callback(err)
    } else {
      callback(null, output)
    }
  })
}

/*
Select training data batch from TABLE using CONNECTION with ID giving offset (id * join_size) in the master table.
Write the data into FOLDER.
Accepts a CALLBACK to pass on errors, otherwise returns nothing.
*/
function select_and_write_training(table, id, folder, connection, callback) {
  const to_write = '/var/lib/docker/volumes/CABINET/_data/DataPipeline/' + folder + '/training_data_' + id.toString() + '.csv'
  const select_training1 = 'WITH temp AS (SELECT * FROM ' + table + ' LIMIT ' + join_size + ' OFFSET ' + (id * join_size) + ') '
  const select_training2 = 'SELECT temp.application as application, temp.code as code, temp.essence as essence, temp.classification as classification, ' + master_precluster_table + '.message as message FROM temp LEFT JOIN ' + cluster_map + ' ON temp.essence = ' + cluster_map + '.essence LEFT JOIN ' + master_precluster_table + ' ON ' + cluster_map + '.hash = ' + master_precluster_table + '.hash'  
  const select_query = select_training1 + select_training2

  async.waterfall([
    // Select training data using double join
    function select(cb) {
      connection.query(select_query, (err, result, fields) => {
        if (err) {
          console.log('CHUNK' + id + ': Error at select')
          cb(err)
        } else {
          ('CHUNK' + id + ': Finished selecting')
          cb(null, result)
        }
      })
    },
    // Write training data
    function write(result, cb) {
      const csvWriter = csv_writer({
        path: to_write,
        header: [
          {id: 'application', title: 'APPLICATION'},
          {id: 'code', title: 'CODE'},
          {id: 'essence', title: 'ESSENCE'},
          {id: 'message', title: 'MESSAGE'},
          {id: 'classification', title: 'CLASSIFICATION'}
        ]
        })

      csvWriter.writeRecords(result)
      .then(() => {
        console.log('CHUNK ' + id + ': Finished writing csv file ' + id)
        callback(null)
      })
      .catch((err) => {
        callback(err)
      })
    }
  ], 
  function (err) {
    if (err) {
      callback(err)
    } else {
      callback(null)
    }
  })
}

/*
Appends TO_WRITE to FILENAME.
Accepts a CALLBACK to pass on errors, otherwise returns nothing.
*/
function append_file(filename, to_write, callback) {
  fs.appendFile(filename, to_write, (err) => {
    if (err) {
      callback(err)
    } else {
      callback(null)
    }
  }) 
}

/*
Creates the create table statement to be written using TABLE_NAME and RESULTS.
Store TABLE_NAME's field information in FIELD_NAMES.
*/
function create_table_statement(table_name, results, field_names) {
  statement = 'CREATE TABLE ' + table_name + ' (\n'
  field_names[table_name] = []
  for (let i = 0; i < results.length; i++) {
    field_names[table_name].push(results[i].Field)
    statement += '   ' + results[i].Field + ' ' + results[i].Type.toUpperCase()
    if (results[i].Key === 'PRI') {
      statement += ' PRIMARY KEY'
    }
    if (i < results.length - 1) {
      statement += ',\n'
    }
    else {
      statement += '\n);\n'
    }
  }
  return {'statement': statement, 'field_names': field_names}
}

/*
Write a single create table statement for TABLE_NAME to FILENAME with FIELD_NAMES.
Uses CONNECTION to query database for TABLE_NAME's field information and FIELD_NAMES to store the data.
Requires a CALLBACK to pass on errors and field_names.
*/
function write_create_table(filename, connection, table_name, field_names, callback) {
  let query = 'DESCRIBE ' + table_name
  connection.query(query, (err, results, fields) => {
    if (err) {
      callback(err)
    } else {
      let ret = create_table_statement(table_name, results, field_names)
      let to_write = ret['statement']
      field_names = ret['field_names']

      append_file(filename, to_write, (err)  => {
        if (err) {
          callback(err)
        }
        else {
          callback(null, field_names)
        }
      })
    }
  })
}

/* 
Write create table statements for TABLE_NAMES into FILENAME. 
Uses CONNECTION to query database for field information and FIELD_NAMES to store the data.
Requires a CALLBACK to pass on errors and field_names.
*/
function write_create_tables(filename, connection, table_names, field_names, callback) {
  if (table_names && table_names.length) {
    let name = table_names.shift()
    write_create_table(filename, connection, name, field_names, (err, ret_fnames) => {
      if (err) {
        console.log('Error making ' + name + ' create table statement.')
        callback(err)
      }
      else {
        console.log('Made ' + name + ' create table statement.')
        write_create_tables(filename, connection, table_names, ret_fnames, callback)
      }
    })
  }
  else {
    callback(null, field_names)
  }
}

/*
Creates the insert into statement to be written using TABLE_NAME and RESULTS and FIELD_NAMES
*/
function insert_into_statement(table_name, result, field_names) {
  let statement = 'INSERT INTO ' + table_name + ' ('
  let cols = field_names[table_name]

  for (let i = 0; i < cols.length; i++) {
    statement += cols[i]
    if (i < cols.length - 1) {
      statement += ', '
    }
    else {
      statement += ')\n'
    }
  }
  statement += 'VALUES\n'

  for (let i = 0; i < result.length; i++) {
    statement += '   ('
    for (let j = 0; j < cols.length; j++) {
      let temp_msg = String(result[i][cols[j]])
      if (temp_msg === null){ 
        statement += 'null'
      }
      else {
        let temp = ''
        if (temp_msg.substring(0,1) === '\"' || temp_msg.substring(0,1) === '\'') {
          temp = temp_msg.substring(1,temp_msg.length - 1)
        }
        else {
          temp = temp_msg
        }
        temp = temp.replace(/"/g, '\\\"')
        temp = temp.replace(/'/g, '\\\'')
        statement += '\'' + temp + '\''
      }
      if (j < cols.length - 1) {
        statement += ', '
      }
      else {
        statement += ')'
      }
    }
    if (i < result.length - 1) {
      statement += ',\n'
    }
    else {
      statement += ';\n'
    }
  }
  return statement
}

/*
Write insert into statements for TABLE_NAME into FILENAME. 
Uses CONNECTION to query database for rows and FIELD_NAMES to store TABLE_NAMES' field data.
Accepts a CALLBACK to pass on errors, otherwise returns nothing.
*/
function write_insert_into(filename, connection, table_name, field_names, callback) {
  let select_query = 'SELECT * FROM ' + table_name
  connection.query(select_query, (err, result, _fields) => {
    if (err) {
      callback(err)
    } else {
      let statement = insert_into_statement(table_name, result, field_names)

      append_file(filename, statement, (err)  => {
        if (err) {
          callback(err)
        }
        else {
          callback(null)
        }
      })      
    }
  })
}

/* 
Write insert into statements for TABLE_NAMES into FILENAME. 
Uses CONNECTION to query database for rows and FIELD_NAMES to store TABLE_NAMES' field data.
Accepts a CALLBACK to pass on errors, otherwise returns nothing.
*/
function write_insert_intos(filename, connection, table_names, field_names, callback) {
  if (table_names && table_names.length) {
    let name = table_names.shift()
    write_insert_into(filename, connection, name, field_names, (err) => {
      if (err) {
        console.log('Error making ' + name + ' insert into statement.')
        callback(err)
      }
      else {
        console.log('Made ' + name + ' insert into statement.')
        write_insert_intos(filename, connection, table_names, field_names, callback)
      }
    })
  }
  else {
    callback(null)
  }
}

/*
Helper function for read flags validation.
Returns 0 if REPLY has valid flags.
Returns 1 if REPLY had too many arguments.
Returns 2 if the flags are not formatted correctly.
*/
function read_flags(reply) {
  const module_flags = ['--modules=all', '--modules=1', '--modules=2', '--modules=3', '--modules=4']
  const rbdms_flags = ['--rbdms=true', '--rbdms=false']
  if (reply.length > 3) {
    return 1
  } else if (reply.length == 3) {
    if ((module_flags.includes(reply[1]) && rbdms_flags.includes(reply[2])) || (module_flags.includes(reply[2] && rbdms_flags.includes(reply[1])))) {
      return 0
    } else {
      return 2
    }
  } else if (reply.length == 2) {
    if (module_flags.includes(reply[1]) || rbdms_flags.includes(reply[1])) {
      return 0
    } else {
      return 2
    }
  } else {
    return 0
  }
}

/*
Helper function for the read option in select_loop.
Takes in LISTENER to grab user input if IO_FNAME is not well formed.
Requires a CALLBACK to return IO_FNAME if valid file and exists, otherwise returns null after too many ATTEMPTS.
*/
function select_loop_read_io(listener, folderpath, io_fname, attempts, callback) {
  if (attempts > 4) {
    callback(null)
  } else {
    let valid = is_alphanum(io_fname)
    let dne = false
    let usable = false
    if (valid) {
      let filepath = folderpath + '/' + io_fname
      exists = fs.existsSync(filepath)
      usable = valid && exists
    }
    if (!usable) {
      if (!valid) {
        console.log()
        console.log('dp: Invalid file name. Please try again.\n')
        listener.question('', (io_fname_1) => {
          select_loop_read_io(listener, folderpath, io_fname_1, attempts + 1, callback)
        })
      } else {
        console.log()
        console.log('dp: File not found. File must be inside the error folder. Please try again.\n')
        listener.question('', (io_fname_1) => {
          select_loop_read_io(listener, folderpath, io_fname_1, attempts + 1, callback)
        })
      }
    } else {
      callback(io_fname)
    }
  }
}

/*
Helper function for the read option in select_loop.
Takes in LISTENER to grab user input if FOLDER_NAME is not well formed.
Returns FOLDER, IO_FNAME if both usable, otherwise returns null after too many ATTEMPTS.
*/
function select_loop_read_fol(listener, folder_name, attempts, callback) {
  if (attempts > 4) {
    callback(null, null)
  } else {
    let valid = is_alphanum(folder_name)
    let dne = false
    let usable = false
    if (valid) {
      let folderpath = __dirname + '/' + folder_name
      exists = fs.existsSync(folderpath)
      usable = valid && exists
    }
    if (!usable) {
      if (!valid) {
        console.log()
        console.log('dp: Invalid file name. Please try again.\n')
        listener.question('', (fol_name_1) => {
          select_loop_read_fol(listener, fol_name_1, attempts + 1, callback)
        })
      } else {
        console.log()
        console.log('dp: Folder must be in the same directory as this file. Please try again.\n')
        listener.question('', (fol_name_1) => {
          select_loop_read_fol(listener, fol_name_1, attempts + 1, callback)
        })
      }
    } else {
      console.log()
      listener.question('dp: Enter the name of the connections file:\n\n', (io_fname) => {
        let folderpath = __dirname + '/' + folder_name
        select_loop_read_io(listener, folderpath, io_fname, 0, (_io_fname) => {
          if (_io_fname) {
            callback(folderpath, _io_fname)
          } else {
            callback(null, null)
          }
        })
      })
    }
  }
}

/*
Helper function for the label option in select_loop.
Takes in LISTENER to grab user input if REPLY is not well formed.
Requires a CALLBACK to return path when user PATH links to a valid file, otherwise returns null after too many ATTEMPTS.
*/
function select_loop_label(listener, path, attempts, callback) {
  if (attempts > 4) {
    callback(null)
  } else {
    if (fs.existsSync(path)) {
      callback(path)
    } else {
      console.log()
      console.log('dp: No such file found. Please try again.\n')
      listener.question('', (path_1) => {
        path = __dirname + '/' + path_1
        select_loop_label(listener, path, attempts + 1, callback)
      })
    }
  }
}

/*
Returns true if STRING is alphanumeric, false otherwise.
*/
function is_alphanum(str) {
  let curr_char
  for (let i = 0; i < str.length; i ++) {
    curr_char = str.charCodeAt(i)
    if (!(curr_char == 95 || curr_char == 46) &&
        !(curr_char > 47 && curr_char < 58) &&
        !(curr_char > 64 && curr_char < 91) &&
        !(curr_char > 96 && curr_char < 123)) {
      return false
    }
  }
  return true
}

/*
Helper function for the get training option in select loop.
Takes in LISTENER to grab user input if FNAME is not well formed.
Requires a CALLBACK to pass on fname when FNAME is a valid filename and does not already exist, otherwise returns null after too many ATTEMPTS
*/
function select_loop_get_tr(listener, fname, attempts, callback) {
  if (attempts > 4) {
    callback(null)
  } else {
    let valid = is_alphanum(fname)
    let dne = false
    let usable = false
    if (valid) {
      const folderpath = '/var/lib/docker/volumes/CABINET/_data/DataPipeline'
      let filepath = folderpath + '/' + fname
      dne = fs.existsSync(filepath)
      usable = valid && !dne
    }
    if (!usable) {
      if (!valid) {
        console.log()
        console.log('dp: Invalid file name. Please try again.\n')
        listener.question('', (fname_1) => {
          select_loop_get_tr(listener, fname_1, attempts + 1, callback)
        })
      } else {
        console.log()
        console.log('dp: Filename already in use. Please try again.\n')
        listener.question('', (fname_1) => {
          select_loop_get_tr(listener, fname_1, attempts + 1, callback)
        })
      }
    } else {
      callback(fname)
    }
  }
}

/*
Helper function for the get table option in select loop.
Takes in LISTENER to grab user input if TABLE is not well formed.
Requires a CALLBACK to return table when TABLE is a valid table name, otheriwse returns null after too many ATTEMPTS.
Performs no checks to see if table actually exists in the database.
*/
function select_loop_get_tab(listener, table, attempts, callback) {
  if (attempts > 4) {
    callback(null)
  } else {
    let valid = is_alphanum(table)
    if (valid) {
      callback(table)
    } else {
      console.log()
      console.log('dp: Invalid table name. Please try again.\n')
      listener.question('', (table_1) => {
        select_loop_get_tab(listener, table_1, attempts + 1, callback)
      })
    }
  }
}

/* 
Helper function for SELECT(). 
Selects which function the call depending on user input REPLY. 
REPLY is an array of the user input delimited by ' '.
Uses LISTENER to read stdin and ATTEMPTS to count calls.
Returns nothing.
*/
function select_loop(listener, reply, attempts) {
  if (reply[0] === 'read') {
    let invalid = read_flags(reply)
    if (invalid) {
      if (invalid == 1) {
        console.log()
        console.log('Input had too many arguments. Read only accepts the --modules and --rdbms flags.')
        select(listener)
      } else {
        console.log()
        console.log('dp: Invalid flag. Read only accepts the --modules and --rbdms flags.')
        select(listener)
      }
    } else {
      let module_flag = 1
      let rbdms_flag = false
      if (reply.length == 3) {
        let first_flag = reply[1]
        let second_flag = reply[2]
        if (first_flag.slice(2,3) == 'm') {
          module_flag = parseInt(first_flag.slice(10))
          rbdms_flag = (second_flag.slice(8) === 'true')
        } else {
          module_flag = parseInt(second_flag.slice(10))
          rbdms_flag = (first_flag.slice(8) === 'true')
        }
      } else if (reply.length == 2) {
        let flag = reply[1]
        if (flag.slice(2,3) == 'm') {
          module_flag = parseInt(flag.slice(10))
        } else {
          rbdms_flag = (flag.slice(8) === 'true')
        }
      }
      if (module_flag >= 2) {
        console.log()
        console.log('dp: BEGINNING READ')
        read(null, null, module_flag, rbdms_flag, (err) => {
          if (err) {
            console.error(err)
          } else {
            console.log()
            console.log('dp: FINISHED READING IN ERRORS.')
          }
        })
      } else {
        listener.question('dp: What is the name of the error folder?\n\n', (folder_name) => {
          select_loop_read_fol(listener, folder_name, 0, (folderpath, io_fname) => {
            if (folderpath) {
              console.log()
              console.log('dp: BEGINNING READ')
              read(folderpath, io_fname, module_flag, rbdms_flag,(err) => {
                if (err) {
                  console.error(err)
                } else {
                  console.log('dp: FINISHED READING IN ERRORS.')
                  process.exit()
                }
              })
            } else {
              console.log()
              console.log('dp: Too many attempts. Perhaps you should try another command.')
              select(listener)
            }
          })
        })
      }
    }
  } else if (reply[0]  === 'label') {
    if (reply.length > 2) {
      console.log()
      console.log('dp: Input had too many arguments. Label only accepts the --get-training flag.')
      select(listener)
    } else if (reply.length == 2 && reply[1] !== '--get-training=true' && reply[1] !== '--get-training=false') {
      console.log()
      console.log('dp: Invalid flag. Label only accepts the --get-training flag.')
      select(listener)
    } else {
      let path = ''
      listener.question('dp: Enter the name of the classified file (do not include the .csv):\n\n', (path_1) => {
        path = __dirname + '/' + path_1
        select_loop_label(listener, path, 0, (path_2) => {
          path = path_2
          if (path) {
            let flag
            if (reply[1]) {
              flag = reply[1].slice(15)
            }
            let get = false
            if (flag && flag === 'true') {
              get = true
            }
            label(path, get, (err) => {
              if (err) {
                console.error(err)
              } else {
                console.log()
                console.log('dp: FINISHED UPDATING LABELS.')
              }
            })
          } else {
            console.log()
            console.log('dp: Too many attempts. Perhaps you should try another command.')
            select(listener)
          }
        })
      })
    }
  } else if (reply[0]  === 'get' && reply[1] === 'training') {
    if (reply.length > 2) {
      console.log()
      console.log('dp: Input had too many arguments. Get training has no flags enabled.')
      select(listener)
    } else {
      listener.question('dp: This file will be written into CABINET/DataPipeline. Enter the file name you would like to write to (do not include the .csv):\n\n', (fname_1) => {
        select_loop_get_tr(listener, fname_1, 0, (fname) => {
          if (fname) {
            get_training(fname, (err) => {
              if (err) {
                console.error(err)
              } else {
                console.log()
                console.log('dp: FINISHED GETTING TRAINING DATA.')
              }
            })
          } else {
            console.log()
            console.log('dp: Too many attempts. Perhaps you should try another command.')
            select(listener)
          }
        })
      })
    }
  } else if (reply[0] === 'get' && reply[1] === 'table') {
    if (reply.length > 2) {
      console.log()
      console.log('dp: Input had too many arguments. Get table has no options enabled.')
      select(listener)
    } else if (reply.length == 2) {
      listener.question('dp: What table would you like to get?\n\n', (table_1) => {
        select_loop_get_tab(listener, table_1, 0, (table) => {
          if (table) {
            get_table(table, (err) => {
              if (err) {
                console.error(err)
              } else {
                console.log('dp: FINISHED GETTING TABLE.')
              }
            })
            console.log('GET TABLE')
          } else {
            console.log()
            console.log('dp: Too many attempts. Perhaps you should try another command.')
            select(listener)
          }
        })
      })
    }
  } else if (reply[0]  === 'dump') {
    if (reply.length > 1) {
      console.log()
      console.log('dp: Input had too many arguments. Dump has no options enabled.')
      select(listener)
    } else {
      const dir = '/var/lib/docker/volumes/LBLR_volume/_data'
      fs.readdir(dir, (err, files) => {
        if (err) {
          console.error(err)
        } else {
          for (file of files) {
            fs.unlink(dir + '/' + file, (err) => {
              if (err) {
                console.error(err)
              }
            })
          }
          dump('backup.sql', (err) => {
            if (err) {
              console.error(err)
            } else {
              console.log()
              console.log('dp: FINISHED SQL DUMP.')
            }
          })
          console.log('DUMP')
        }
      })
    }
  } else if (reply[0]  == 'help') {
    if (reply.length > 1) {
      console.log()
      console.log('dp: Input had too many arguments. Help function has no options enabled.')
      select(listener)
    } else {
      console.log(helptext)
      select(listener)
    }
  } else if (reply[0] == 'exit') {
    console.log()
    console.log('dp: EXITING')
    process.exit()
  } else {
    console.log()
    console.log('dp: Command not recognized. Please try again.\n')
    if (attempts == 2) {
      console.log('dp: Enter help to see available options.\n')
      listener.question('', (_reply) => {
        console.log()
        select_loop(listener, _reply.split(' '), attempts + 1)
      })
    } else {
      listener.question('', (_reply) => {
        console.log()
        select_loop(listener, _reply.split(' '), attempts + 1)
      })
    }
  }
}


// READ MODULE FUNCTIONS --------------------------------------------------------------------------------------------------------------------
// The read functions is split into modules which serve as checkpoints for various steps in the pipeline. 

/*
MODULE 1: find files, connect, create userid, create io, insert io, process batches, drop io
FOLDERPATH is the path to the error dump, IO_PATH the path to the IO connections file, CONNECTION is a connection to the database.
On completion module 1 serves as a checkpoint for processing the json files and uploading the errors into userid table.
If an error occcurs afterwards read() may continue from module 2 using only the data inside userid table.
Requires a callback to pass on errors, otherwise immediately calls module 2.
*/
function module1(folderpath, io_path, connection, callback) {
  async.waterfall([
    // SETUP: find files, connect, create userid, create io, insert io (result = [batches, null, null], returns batches)
    function setup(cb) {
      async.parallel([
        // Find the json files to be processed (returns batches)
        function find_files(pcb) {
          fs.readdir(folderpath, function(err, result) {
            if (err) {
              console.log('Error at find_files().')
              pcb(err)
            } else {
              let json_filepaths = []
              for (filename of result) {  
                if (filename.endsWith('.json')) {
                  json_filepaths.push(folderpath + '/' + filename)
                }
              }
    
              const batches = []
              const num_batches = Math.floor(json_filepaths.length / batch_size)
              for (let i = 0; i < num_batches; i ++) {
                batches.push(json_filepaths.slice(0, batch_size))
                json_filepaths.splice(0, batch_size)
              }
              batches.push(json_filepaths)
              
              console.log('Sorted folder: ' + folderpath + ': found ' + batches.length + ' batches.')
              pcb(null, batches)
            }
          })
        },
        // Create userid_table (returns null)
        function create_uuid(pcb) {
          create_table(userid_table, userid_example, connection, (err, result) => {
            if (err) {
              console.log('Failed to create table: ' + userid_table)
              pcb(err)
            } else {
              console.log('Finished creating table: ' + userid_table)
              pcb(null)
            }
          })
        },
        // Submodule 1: create io, insert io (returns null)
        function submodule1(pcb) {
          async.waterfall([
            // Create io table.
            function create_io(sm_wcb) {
              create_table(io_table, io_example, connection, (err, result) => {
                if (err) {
                  console.log('Failed to create table: ' + io_table)
                  sm_wcb(err)
                } else {
                  console.log('Finished creating table: ' + io_table)
                  sm_wcb(null)
                }
              })
            },
            // Insert io file.
            function insert_io(sm_wcb) {
              read_csvfile(io_path, (err, result) => {
                if (err) {
                  sm_wcb(err)
                } else {  
                  const headers = ['application', '_ID']
                  const data = result.slice(1)
                  const new_data = []
                  for (d of data) {
                    nd = []
                    if (d[1]) {
                      nd.push(d[1])
                    } else {
                      nd.push(d[0])
                    }
                    nd.push(d[3])
                    new_data.push(nd)
                  }
                  const io_write = make_obj(headers, new_data)
                  insert_table(io_write, io_table, connection, null, (err) => {
                    if (err) {
                      sm_wcb(err)
                    } else {
                      sm_wcb(null)
                    }
                  })
                }
              })
            }
          ], 
          function (err) {
            if (err) {
              pcb(err)
            } else {
              pcb(null)
            }
          })
        }
      ], 
      function (err, result) {
        if (err) {
          cb(err)
        } else {
          cb(null, result[0])
        }
      })
    },
    // Process batches (returns null)
    function process(batches, cb) {
      batch_sync(batches, userid_table, connection, (err, output) => {
        if (err) {
          cb(err)
        } else {
          cb(null, output)
        }
      })
    },
    // Drop io_table (returns null)
    function drop_io(output, cb) {
      drop_table(io_table, connection, (err, result) => {
        if (err) {
          console.log('Error dropping: ' + io_table)
          cb(err)
        } else {
          console.log('Dropped: ' + io_table)
          cb(null, output)
        }
      })
    },
  ], 
  function (err, output) {
    if (err) {
      callback(err)
    } else {
      module2(output, connection, callback)
    }
  })
}

/*
Module 2: select userid, sort precluster, insert precluster, flask convert, write flask
USERID DATA is the data from userid table. CONNECTION is a connnection to the database.
On completion module 2 serves as a checkpoint for the flask processing of messages into essences.
If an error occurs afterwards read() may continue from module 3 using only the data written to postflask.csv
Requires a callback to pass on errors, otherwise immediately calls module 3.
*/
function module2(userid_data, connection, callback) {
  async.waterfall([
    // Select from userid table (returns result)
    function select_userid(wcb) {
      if (userid_data) {
        console.log('Passed userid table data')
        wcb(null, userid_data)
      } else {
        const cols = comma(relevant_fields)
        const select_query = 'SELECT ' + cols + ', hash FROM ' + userid_table + ' GROUP BY '+ cols + ', hash'
    
        connection.query(select_query, (err, result, fields) => {
          if (err) {
            console.log('Error at select_uuid().')
            wcb(err)
          } else {
            console.log('Finished select userid: found ' + result.length + ' unique errors.') 
            wcb(null, result)
          }
        })
      }
    },
    // Sort into seen and new using preclusters rows (returns _new)
    function sort_precluster(precluster_data, wcb) {
      const hash_select = 'SELECT hash FROM ' + master_precluster_table
      
      connection.query(hash_select, (err, result, fields) => {
        if (err) {
          console.log('Error at sort_precluster().')
          wcb(err)
        } else {
          const hashes = []
          for (hash_obj of result) {
            hashes.push(hash_obj['hash'])
          }
          const _new = []
          for (error of precluster_data) {
            if (!hashes.includes(error['hash'])) {
              _new.push(error)
            }
          }
          console.log('Finished sorting precluster errors: found ' + _new.length + ' new errors.')
          wcb(null, _new)
        }
      })
    },
    // Submodule 2: insert precluster, flask convert, write flask (result = [null, flasked], returns flasked)
    function submodule2(_new, wcb) {
      async.parallel([
        // Insert _new into master_precluster (returns null)
        function insert_precluster(sm2_pcb) {
          if (_new && _new.length) {
            const error_keys = ['message', 'hash']
            const to_insert = []
            for (error of _new) {
              const error_values = {}
              for (key of error_keys) {
                error_values[key] = error[key]
              }
              to_insert.push(error_values)
            }

            insert_table(to_insert, master_precluster_table, connection, null, (err) => {
              if (err) {
                console.log('Error at insert precluster.')
                sm2_pcb(err)
              } else {
                console.log('Finished inserting new errors into ' + master_precluster_table + '.')
                sm2_pcb(null)
              }
            })
          } else {
            console.log('Finished inserting new errors into ' + master_precluster_table + ' (none found)')
            sm2_pcb(null)
          }
        },
        // Submodule 2a: flask convert, write flask (returns flasked)
        function submodule2a(sm2_pcb) {
          async.waterfall([
            // Send data to Flask server (returns flasked)
            function flask(sm2a_wcb) {
              const startFlask = new Date()
              const messages = []
              for (datum of _new) {
                let temp = {}
                temp['message'] = datum['message']
                messages.push(temp)
              }

              console.log('Converted data for flask: Collected ' + messages.length + ' messages.')

              flask_convert(messages, (err, body) => {
                if (err) {
                  console.log('Error at flask service.')
                  sm2a_wcb(err)
                } else {
                  const endFlask = new Date()
                  const execFlask = time_string(endFlask - startFlask)
                  console.log('FLASK EXECUTION TIME: ' + execFlask)
                  
                  const flasked = []
                  let length = body.length
                  console.log('Received ' + body.length + ' essences.')
                  for (let i = 0; i < length; i ++) {
                    if (body[i]['essence'] !== 'TRUNCATED' && body[i]['essence'] !== '') {
                      _new[i]['essence'] = body[i]['essence']
                      flasked.push(_new[i])
                    }
                  }
                  console.log('Found ' + flasked.length + ' untruncated errors.')
                  sm2a_wcb(null, flasked)
                }
              })
            },
            // Write results to postflask.csv (returns flasked)
            function write_flask(flasked, sm2a_wcb) {
              const csvWriter = csv_writer({
                path: __dirname + '/postflask.csv',
                header: [
                    {id: 'application', title: 'application'},
                    {id: 'code', title: 'code'},
                    {id: 'message', title: 'message'},
                    {id: 'essence', title: 'essence'},
                    {id: 'hash', title: 'hash'}
                ]
              })
      
              csvWriter.writeRecords(flasked)
              .catch((err) => {
                console.log('Error writing: postflask.csv')
                sm2a_wcb(err)
              })
              .then(() => {
                console.log('Finished writing: postflask.csv')
                sm2a_wcb(null, flasked)
              })
            }
          ], 
          function (err, flasked) {
            if (err) {
              sm2_pcb(err)
            } else {
              sm2_pcb(null, flasked)
            }
          })
        }
      ], 
      function (err, result) {
        if (err) {
          wcb(err)
        } else {
          wcb(null, result[1])
        }
      })
    }
  ],
  function (err, result) {
    if (err) {
      callback(err)
    } else {
      module3(result, connection, callback)
    } 
  })

}

/*
MODULE 3: t1 cluster, sort master, insert master, insert cluster, update cluster, t2 cluster, write t2
FLASKED is the errors combined with the essences returned from the flask server. CONNECTION is a connection to the database. 
On completion module 3 serves as a checkpoint for the clustering of errors.
If an error occure afterwards read() may continue from module 4 using only the data written to t2_clusters.csv
Requires a callback to pass on errors, otherwise immediately calls module 4.
*/
function module3(flasked, connection, callback) {
  async.waterfall([
    // Gets postflask.csv data if not passed along.
    function setup(cb) {
      if (flasked) {
        cb(null, flasked)
      } else {
        read_csvfile(__dirname + '/postflask.csv', (err, flasked_data) => {
          if (err) {
            cb(err)
          } else {
            const flasked_data_objs = make_obj(flasked_data[0], flasked_data.slice(1))
            cb(null, flasked_data_objs)
          }
        })
      }
    },
    // Clusters by essence equality
    function t1_cluster(flasked, cb) {
      const clusters = t1_clustering(flasked)
      const master_data = []
      const cluster_mappings = []

      for (cluster of clusters) {
        master_data.push(cluster[0])
        let key = cluster[0]['essence']
        for (e of cluster) {
          let map = {}
          map['essence'] = key
          map['hash'] = e['hash']
          cluster_mappings.push(map)
        }
      }
      console.log('Finished Tier 1 clustering')
      cb(null, master_data, cluster_mappings)
    },
    // Submodule 3: sort master, insert master, t2 cluster, write t2, update cluster (result = [t2_clusters, null], return t2_clusters)
    function submodule3(master_insert, cluster_mappings, cb) {
      async.parallel([
        // Submodule 3a: sort master, insert master, t2 cluster, write t2 (returns t2_clusters)
        function submodule3a(sm3_pcb) {
          async.waterfall([
            // Sorts clusters by essence from master table
            function sort_master(sm3a_wcb) {
              const essence_select = 'SELECT essence FROM ' + master_table
                  
              connection.query(essence_select, (err, result, fields) => {
                if (err) {
                  console.log('Error at sort_master().')
                  sm3a_wcb(err)
                } else {
                  const essences = []
                  for (essence_obj of result) {
                    essences.push(essence_obj['essence'])
                  }

                  const new_master = []

                  for (md of master_insert) {
                    let curr_ess = md['essence']
                    if (!essences.includes(curr_ess)) {
                      new_master.push(md)
                    }
                  }

                  console.log('Finished sorting master errors: found ' + new_master.length + ' new errors.')
                  sm3a_wcb(null, new_master)
                }
              })
            },
            // Submodule 3b: insert master, t2 cluster, write t2, create run, insert run (result = [null, t2_clusters, null], returns t2_clusters)
            function submodule3b(new_master, sm3a_wcb) {
              async.parallel([
                // Insert new errors into master (returns null)
                function insert_master(sm3b_pcb) {
                  if (new_master && new_master.length) {
                    const to_insert = []
                    for (error of new_master) {
                      let error_values = {}
                      for (c of master_cols) {
                        error_values[c] = error[c]
                      }
                      to_insert.push(error_values)
                    }

                    insert_table(to_insert, master_table, connection, null, (err) => {
                      if (err) {
                        console.log('Error at insert master.')
                        sm3b_pcb(err)
                      } else {
                        console.log('Finished inserting new errors into: ' + master_table + '.')
                        sm3b_pcb(null)
                      }
                    })
                  } else {
                    console.log('Finished inserting new errors into: ' + master_table  + ' (none found)')
                    sm3b_pcb(null)
                  }
                },
                // Submodule 3c: t2 cluster, write t2 (returns t2_clusters)
                function submodule3c(sm3b_pcb) {
                  async.waterfall([
                    // Implements tier 2 clustering by essence
                    function t2_cluster(sm3c_wcb) {
                      t2_convert(new_master, (err, t2_clusters) => {
                        if (err) {
                          console.log('Error at Tier 2 clustering.')
                          sm3c_wcb(err)
                        } else {
                          console.log('Finished Tier 2 clustering.')
                          sm3c_wcb(null, t2_clusters)
                        }
                      })
                    },
                    // Write t2 clusters
                    function write_t2(t2_clusters, sm3c_wcb) {
                      fs.writeFile(__dirname + '/t2_clusters.json', JSON.stringify(t2_clusters), (err) => {
                        if (err) {
                          sm3c_wcb(err)
                        } else {
                          sm3c_wcb(null, t2_clusters)
                        }
                      })
                    }
                  ], 
                  function (err, result) {
                    if (err) {
                      sm3b_pcb(err)
                    } else {
                      sm3b_pcb(null, result)
                    }
                  })
                },
                // Submodule 3d: create run, insert run (returns null)
                function submodule3d(sm3b_pcb) {
                  async.waterfall([
                    // Create run map
                    function create_run(sm3d_wcb) {
                      create_table(run_map, run_example, connection, (err) => {
                        if (err) {
                          console.log('Error creating: ' + run_map)
                          sm3d_wcb(err)
                        } else {
                          console.log('Created table: ' + run_map)
                          sm3d_wcb(null)
                        }
                      })
                    },
                    // Insert into run map
                    function insert_run(sm3d_wcb) {
                      const to_insert = []
                      for (error of new_master) {
                        let error_to_insert = {}
                        error_to_insert['essence'] = error['essence']
                        error_to_insert['hash'] = error['hash']
                        to_insert.push(error_to_insert)
                      }
                      insert_table(to_insert, run_map, connection, null, (err) => {
                        if (err) {
                          console.log('Error inserting into: ' + run_map)
                          sm3d_wcb(err)
                        } else {
                          console.log('Finished insert into ' + run_map)
                          sm3d_wcb(null)
                        }
                      })
                    }
                  ], 
                  function (err) {
                    if (err) {
                      sm3b_pcb(err)
                    } else {
                      sm3b_pcb(null)
                    }
                  })
                }
              ], 
              function (err, result) {
                if (err) {
                  sm3a_wcb(err)
                } else {
                  sm3a_wcb(null, result[1])
                }
              })
            }
          ], 
          function (err, result) {
            if (err) {
              sm3_pcb(err)
            } else {
              sm3_pcb(null, result)
            }
          })
        },
        // Insert values into cluster_map (returns null)
        function insert_cluster(sm3_pcb) {
          if (cluster_mappings && (cluster_mappings.length)) {
            insert_table(cluster_mappings, cluster_map, connection, true, (err) => {
              if (err) {
                console.log('Error inserting into: ' + cluster_map)
                sm3_pcb(err)
              } else {
                console.log('Finished inserting into: ' + cluster_map)
                sm3_pcb(null)
              }
            })
          } else {
            console.log('Finished inserting into: ' + cluster_map  + ' (none found)')
            sm3_pcb(null)
          }
        }
      ], 
      function(err, result) {
        if (err) {
          cb(err)
        } else { 
          cb(null, result[0])
        }
      })
    }
  ],
  function (err, result) {
    if (err) {
      callback(err)
    } else {
      module4(result, connection, callback)
    }
  })
}

/*
MODULE 4: count userid, write csv, drop userid
T2 CLUSTERS are the Tier 2 Clusters. CONNECTION is a connection to the database.
Module 4 is the final module.
Accepts a callback to pass on errors, otheriwse returns nothing.
*/
function module4(t2_clusters, connection, callback) {
  async.waterfall([
    function setup(cb) {
      if (t2_clusters) {
        console.log('Passed on T2 Clusters.')
        cb(null, t2_clusters)
      } else {
        recover_clusters(__dirname + '/t2_clusters.json', (err, clusters) => {
          if (err) {
            cb(err)
          } else {
            cb(null, clusters)
          }
        })
      }
    },
    // Count userids by cluster
    function count_userid(clusters, cb) {
      get_prototypes(clusters, connection, (err, prototypes) => {
        if (err) {
          console.log('Error at count userid')
          cb(err)
        } else {
          console.log('Found ' + prototypes.length + ' prototypes.') 
          cb(null, prototypes)
        }
      })
    },
    // Write to classify csv file
    function write_csv(prototypes, cb) {
      const csvWriter = csv_writer({
        path: __dirname + '/to_classify.csv',
        header: [
            {id: 'application', title: 'application'},
            {id: 'code', title: 'code'},
            {id: 'message', title: 'message'},
            {id: 'count', title: 'count'},
            {id: 'userids', title: 'userids'}
        ]
      })

      csvWriter.writeRecords(prototypes)
      .catch((err) => {
        console.log('Error writing: to_classify.csv')
        cb(err)
      })
      .then(() => {
        console.log('Finished writing: to_classify.csv')
        cb(null)
      })
    },
    // Drop userid table
    function drop_userid(cb) {
      drop_table(userid_table, connection, (err) => {
        if (err) {
          console.log('Error dropping: ' + userid_table)
          cb(err)
        } else {
          console.log('Dropped ' + userid_table)
          cb(null)
        }
      })
    }
  ], 
  function (err) {
    if (err) {
      callback(err)
    } else {
      callback(null)
    }
  })
}


// MAIN FUNCTIONS ---------------------------------------------------------------------------------------------------------------------------

/*
Reads in the json error files inside FOLDERPATH and adds the relevant application using IO_NAME.
Starts with START_MODULE. If RBDMS = true, uses the rbdms file to add in relevant RBDMS information to application.
Accepts a callback to pass on errors, otherwise returns nothing.
*/
function read(folderpath, io_name, start_module=1, rbdms=false, callback) {
  async.waterfall([
    // Connect to database
    function connect(cb) {
      connect_db((err, connection) => {
        if (err) {
          console.log('Error connection to database.')
          cb(err)
        } else {
          console.log('Connected to database with id: ' + connection.threadId)
          cb(null, connection)
        }
      })
    },
    // Select which module to start from
    function modules(connection, cb) {
      if (start_module == 1) {
        const io_path = folderpath + '/' + io_name
        module1(folderpath, io_path, connection, (err) => {
          if (err) {
            cb(err)
          } else {
            cb(null, connection)
          }
        })
      } else if (start_module == 2) {
        module2(null, connection, (err) => {
          if (err) {
            cb(err)
          } else {
            cb(null, connection)
          }
        })
      } else if (start_module == 3) {
        module3(null, connection, (err) => {
          if (err) {
            cb(err)
          } else {
            cb(null, connection)
          }
        })
      } else {
        module4(null, connection, (err) => {
          if (err) {
            cb(err)
          } else {
            cb(null, connection)
          }
        })
      } 
    },
    // Cleanup function
    function cleanup(connection, cb) {
      console.log('Start clean up.')
      async.parallel([
        // Clean up database: drop run, close connection
        function database(pcb) {
          async.waterfall([
            // Drop run map
            function drop_run(wcb) {
              drop_table(run_map, connection, (err) => {
                if (err) {
                  console.log('Error dropping run map.')
                  wcb(err)
                } else {
                  console.log('Dropped run map')
                  wcb(null)
                }
              })
            },
            // Close connection to database
            function close(wcb) {
              connection.end(function(err) {
                if (err) {
                  console.log('Error closing connection to databse.')
                  wcb(err)
                } else {
                  console.log('Closed connection to database.')
                  wcb(null)
                }
              })
            }
          ], 
          function (err) {
            if (err) {
              pcb(err)
            } else {
              pcb(null)
            }
          })
        },
        function delete_t2(pcb) {
          if (fs.existsSync(__dirname + '/t2_clusters.json')) {
            fs.unlink(__dirname + '/t2_clusters.json', (err) => {
              if (err) {
                console.log('Error cleaning up t2_clusters.json')
                pcb(err)
              } else {
                console.log('Finished cleaning up t2_clusters.json')
                pcb(null)
              }
            })
          } else {
            console.log('Finished cleaning up t2_clusters.json')
            pcb(null)
          }
        },
        // Delete postflask.csv if exists
        function delete_pf(pcb) {
          if (fs.existsSync(__dirname + '/postflask.csv')) {
            fs.unlink(__dirname + '/postflask.csv', (err) => {
              if (err) {
                console.log('Error cleaning up postflask.csv')
                pcb(err)
              } else {
                console.log('Finished cleaning up postflask.csv')
                pcb(null)
              }
            })
          } else {
            console.log('Finished cleaning up postflask.csv')
            pcb(null)
          }
        },
      ], 
      function (err) {
        if (err) {
          console.log('Error at cleanup.')
          cb(err)
        } else {
          console.log('Finished cleaning up.')
          cb(null)
        }
      })
    }
  ], 
  function (err) {
    if (err) {
      callback(err)
    } else {
      callback(null)
    }
  })
} 

/*
Updates the master table using the labels from CLASSIFIED_FILEPATH. 
If GET == TRUE then write new training data using the updated labels.
Accepts a CALLBACK to pass on errors, otherwise returns nothing.
*/
function label(classified_filepath, get=true, callback) {
  async.waterfall([
    // Read classified.csv
    function read(cb) {
      read_csvfile(classified_filepath, (err, file_data) => {
        if (err) {
          console.log('Error reading: ' + classified_filepath)
          cb(err)
        } else {
          console.log('Read in: ' + classified_filepath)
          cb(null, file_data)
        }
      })
    },
    // Sort classified.csv
    function sort_classified(file_data, cb) {
      const data = []
      const headers = file_data[0]
      data.push(headers)
      let label_index = 0
      for (h of headers.split(',')) {
        if (h === 'classification') {
          break
        } else {
          label_index += 1
        }
      }
      for (row of file_data.slice(1)) {
        if (row[label_index] !== 'none') {
          data.push(row)
        }
      }
      console.log('Finished sorting errors')
      cb(null, data)
    },
    // Connect to database
    function connect(data, cb) {
      connect_db((err, connection) => {
        if (err) {
          console.log('Error connection to MySQL database.')
          cb(err)
        } else {
          console.log('Connected to MySQL database with ID: ' + connection.threadId)
          cb(null, data, connection)
        }
      })
    },
    // Send messages to flask server to get essences
    function get_essences(data, connection, cb) {
      const startFlask = new Date()
      let message_index = 0
      for (h of data[0].split(',')) {
        if (h === 'message') {
          break
        } else {
          label_index += 1
        }
      }
      let label_index = 0
      for (h of data[0].split(',')) {
        if (h === 'classification') {
          break
        } else {
          label_index += 1
        }
      }
      const messages = []
      for (row of data.slice(1)) {
        let temp = {}
        temp['message'] = datum[message_index]
        messages.push(temp)
      }

      console.log('Converted data for flask: Collected ' + messages.length + ' messages.')

      flask_convert(messages, (err, body) => {
        if (err) {
          console.log('Error at flask server.')
          cb(err)
        } else {
          const endFlask = new Date()
          const execFlask = timeString(endFlask - startFlask)
          console.log('Flask execution time: ' + execFlask)
          console.log('Received ' + body.length + ' essences.')

          const to_update = {}
          for (let i = 0; i < body.length; i++) {
            to_update[body[i]['essence']] = data[i][label_index]
          }
          cb(null, to_update, connection)
        }
      })
    },
    // Update master table with labels using async.parallel
    function update_labels(to_update, connection, cb) {
      const update_query1 = 'UPDATE ' + master_table + ' SET classification = "'
      const update_query2 = '" WHERE essence = "'

      const essences = []
      for (e of Object.keys(to_update)) {
        essences.push(e)
      }
      async.map(essences, 
      (e, map_callback) => {
        let update_query = update_query1 + to_update[e] + update_query2 + e + '"'
        connection.query(update_query, (err, result) => {
          if (err) {
            map_callback(err)
          } else {
            map_callback(null)
          }
        })
      },
      (err, result) => {
        if (err) {
          console.log('Error updating labels.')
          cb(err)
        } else {
          console.log('Finish updating labels.')
          cb(null, to_update, connection, cb)
        }
      })
    },
    // Create training data from new labels if get_training = true
    function get_training(to_update, connection, cb) {
      if (get == true) {
        const temp_table = 'relevant_master'
        async.waterfall([
          // Select master table
          function select_master(wcb) {
            const select_query = 'SELECT * FROM master'
            connection.query(select_query, (err, result) => {
              if (err) {
                console.log('Error selecting from: ' + master_table)
                wcb(err)
              } else {
                console.log('Finished selecting from: ' + master_table)
                wcb(null, result)
              }
            })
          },
          // Sort master table
          function sort_master(master_data, wcb) {
            const essences = Object.keys(to_update)
            let to_get = []
            for (data_obj of master_data) {
              if (essences.includes(data_obj['essence'])) {
                to_get.push(data_obj)
              }
            }
            wcb(null, to_get)
          },
          // Create temp table
          function create_temp(to_get, wcb) {
            create_table(temp_table, to_get[0], connection, (err) => {
              if (err) {
                wcb(err)
              } else {
                wcb(null, to_get)
              }
            })
          },
          // Insert data into temp
          function insert_temp(to_get, wcb) {
            insert_table(to_get, temp_table, connection, null, (err) => {
              if (err) {
                wcb(err)
              } else {
                wcb(null)
              }
            })
          },
          // Make directory for training data batches
          function mkdir(wcb) {
            const folder = '/var/lib/docker/volumes/CABINET/_data/DataPipeline/td_temp'
            fs.mkdir(folder, (err) => {
              if (err) {
                wcb(err)
              } else {
                wcb(null, folder)
              }
            })
          },
          // Join temp with cluster_map, master_precluster, then write to a file, in chunks
          function join_and_write_training(folder, wcb) {
            const select_query = 'SELECT COUNT(*) as count FROM ' + temp_table
            connection.query(select_query, (err, result) => {
              if (err) {
                wcb(err)
              } else {
                const count = result[0]['count']
                const num_chunks = Math.ceil(count / join_size)
                const chunk_list = []
                for (let i = 0; i < num_chunks; i ++) {
                  chunk_list.push(i)
                }
                async.map(chunk_list, 
                (i, map_callback) => {
                  select_and_write_training(master_table, i, folder, connection, (err) => {
                    if (err) {
                      map_callback(err)
                    } else {
                      map_callback(null)
                    }
                  })
                },
                (err, result) => {
                  if (err) {
                    cb(err)
                  } else {
                    cb(null)
                  }
                })
              }
            })
          },
          // Aggregate training data
          function aggregate(folder, wcb) {
            async.waterfall([
              function read_files(w1cb) {
                const filepaths = fs.readdirSync(folder)
                read_csvfiles(filepaths, (err, output) => {
                  if (err) {
                    w1cb(err)
                  } else {
                    w1cb(output)
                  }
                })
              },
              function write_file(output, w1cb) {
                const csvWriter = csv_writer({
                  path: '/var/lib/docker/volumes/CABINET/_data/DataPipeline/' + filename,
                  header: [
                    {id: 'application', title: 'APPLICATION'},
                    {id: 'code', title: 'CODE'},
                    {id: 'essence', title: 'ESSENCE'},
                    {id: 'message', title: 'MESSAGE'},
                    {id: 'classification', title: 'CLASSIFICATION'}
                  ]
                  })
          
                csvWriter.writeRecords(output)
                .then(() => {
                  console.log('Finished writing csv file ' + id)
                  w1cb(null)
                })
                .catch((err) => {
                  w1cb(err)
                })
              }
            ], 
            function (err) {
              if (err) {
                wcb (err)
              } else {
                wcb(null, folder, connection)
              }
            })
          },
          // Delete folder
          function rmdir(folder, wcb) {
            fs.rmdir(folder, { recursive: true }, (err) => {
              if (err) {
                wcb(err)
              } else {
                wcb(null)
              }
            })
          },
          // Delete temp table
          function delete_temp(wcb) {
            drop_table(temp_table, connection, (err) => {
              if (err) {
                wcb(err)
              } else {
                wcb(null)
              }
            })
          }
        ], 
        function (err) {
          if (err) {
            cb(err)
          } else {
            cb(null, connection)
          }
        })
      } else {
        cb(null, connection)
      }
    },
    // Close connection 
    function close(connection, cb) {
      connection.end(function(err) {
        if (err) {
          console.log('Error closing connection to databse.')
          cb(err)
        } else {
          console.log('Closed connection to database.')
          cb(null)
        }
      })
    }
  ], 
  function (err) {
    if (err) {
      callback(err)
    }
  })
}

/*
Gets all of the training data and writes it to FILENAME inside CABINET/DataPipeline.
Accepts a CALLBACK to pass on errors, otherwise returns nothing. 
*/
function get_training(filename, callback) {
  console.log('GETTING TRAINING DATA.')
  async.waterfall([
    // Connect to database
    function connect(cb) {
      connect_db((err, connection) => {
        if (err) {
          console.log('BATCH ' + id + ': Error connection to MySQL database.')
          cb(err)
        } else {
          console.log('BATCH ' + id + ': Connected to MySQL database with ID: ' + connection.threadId)
          cb(null, connection)
        }
      })
    },
    // Make folder
    function mkdir(connection, cb) {
      const folder = '/var/lib/docker/volumes/CABINET/_data/DataPipeline/td_temp'
      fs.mkdir(folder, (err) => {
        if (err) {
          cb(err)
        } else {
          cb(null, folder, connection)
        }
      })
    },
    // Select and write training
    function select_and_write(folder, connection, cb) {
      const select_query = 'SELECT COUNT(*) as count FROM ' + master_table
      connection.query(select_query, (err, result) => {
        if (err) {
          cb(err)
        } else {
          const count = result[0]['count']
          const num_chunks = Math.ceil(count / join_size)
          const chunk_list = []
          for (let i = 0; i < num_chunks; i ++) {
            chunk_list.push(i)
          }
          async.map(chunk_list, 
          (i, map_callback) => {
            select_and_write_training(master_table, i, folder, connection, (err) => {
              if (err) {
                map_callback(err)
              } else {
                map_callback(null)
              }
            })
          },
          (err, res) => {
            if (err) {
              cb(err)
            } else {
              cb(null)
            }
          })
        }
      })
    },
    // Aggregate files
    function aggregate(folder, connection, cb) {
      async.waterfall([
        function read_files(wcb) {
          const filepaths = fs.readdirSync(folder)
          read_csvfiles(filepaths, (err, output) => {
            if (err) {
              wcb(err)
            } else {
              wcb(output)
            }
          })
        },
        function write_file(output, wcb) {
          const csvWriter = csv_writer({
            path: '/var/lib/docker/volumes/CABINET/_data/DataPipeline/' + filename,
            header: [
              {id: 'application', title: 'APPLICATION'},
              {id: 'code', title: 'CODE'},
              {id: 'essence', title: 'ESSENCE'},
              {id: 'message', title: 'MESSAGE'},
              {id: 'classification', title: 'CLASSIFICATION'}
            ]
            })
    
          csvWriter.writeRecords(output)
          .then(() => {
            console.log('Finished writing csv file ' + id)
            wcb(null)
          })
          .catch((err) => {
            wcb(err)
          })
        }
      ], 
      function (err) {
        if (err) {
          cb (err)
        } else {
          cb(null, folder, connection)
        }
      })
    },
    // Delete folder
    function rmdir(folder, connection, cb) {
      fs.rmdir(folder, { recursive: true }, (err) => {
        if (err) {
          cb(err)
        } else {
          cb(null, connection)
        }
      })
    },
    // Close connection
    function close(connection, cb) {
      connection.end(function(err) {
        if (err) {
          console.log('Error closing connection to databse.')
          cb(err)
        } else {
          console.log('Closed connection to database.')
          cb(null)
        }
      })
    }
  ], 
  function (err) {
    if (err) {
      callback(err)
    } else {
      callback(null)
    }
  })
}

/*
Write TABLE_NAME in the databse into a csv file.
Accepts a CALLBACK to pass on errors, otherwise returns nothing. 
*/
function get_table(table_name, callback) {
  console.log()
  console.log('dp: BEGIN GET TABLE')
  async.waterfall([
    function connect(cb) {
      connect_db((err, connection) => {
        if (err) {
          console.log('Error connecting to database')
          cb(err)
        } else {
          console.log('Connected to database with id: ' + connection.threadId)
          cb(null, connection)
        }
      })
    },
    function select(connection, cb) {
      const select_query = 'SELECT * FROM ' + table_name
      connection.query(select_query, (err, result, fields) => {
        if (err) {
          console.log('Error selecting from: ' + table_name)
          cb(err)
        } else {
          console.log('Selected all ' + result.length + ' rows from: ' + table_name)
          cb(null, result, connection)
        }
      })
    },
    function write(to_write, connection, cb) {
      _headers = []
      const keys = Object.keys(to_write[0])
      for (k of keys) {
        _headers.push(k)
      }

      const records = []
      for (tw of to_write) {
        curr_record = []
        for (k of keys) {
          curr_record.push(tw[k])
        }
        records.push(curr_record)
      }

      const csvWriter = csv_writer_arr({
        path: __dirname + '/' + table_name + '.csv',
        header: _headers
      })

      csvWriter.writeRecords(records)
      .then(() => {
        console.log('Finished writing csv file')
        cb(null, connection)
      })
      .catch((err) => {
        console.log('Failed to write.\n')
        cb(err)
      })
    },
    function close(connection, cb) {
      connection.end(function(err) {
        if (err) {
          console.log('Error closing connection to databse.')
          cb(err)
        } else {
          console.log('Closed connection to database.')
          cb(null)
        }
      })
    }
  ], 
  function (err, result) {
    if (err) {
      callback(err)
    } else {
      callback(null)
    }
  })
}

/*
Writes a SQL dump of all tables currently in the database to FILENAME.
Accepts a CALLBACK to pass on errors, otherwise returns nothing.
*/
function dump(file, callback) {
  const filename = '/var/lib/docker/volumes/LBLR_volume/_data/' + file
  async.waterfall([
    // Create file to write to
    function create_file(cb) {
      fs.writeFile(filename, '', (err) => {
        if (err) {
          console.log('Error creating file.')
          cb(err)
        }
        else {
          console.log('Finished creating file.')
          cb(null)
        }
      })
    },
    // Connect to database
    function connect(cb) {
      connect_db((err, connection) => {
        if (err) {
          console.log('Error connecting to MySQL database.')
          cb(err)
        } else {
          console.log('Connected to MySQL database with ID: ' + connection.threadId)
          cb(null, connection)
        }
      })
    },
    // Get list of table names
    function get_table_names(connection, cb) {
      let table_query = 'SHOW TABLES'
      
      connection.query(table_query, (err, results, fields) => {
        if (err) {
          console.log('Error querying cluster_map column info.')
          cb(err)
        }
        else {
          console.log('Queried table name info.')
          let table_names = []
          for (result of results) {
            table_names.push(result[fields[0].name])
          }
          cb(null, connection, table_names)
        }
      })
    },
    // Write create table statements
    function write_createTable(connection, table_names, cb) {
      write_create_tables(filename, connection, table_names.slice(0), {}, (err, field_names) => {
        if(err) {
          console.log('Error writing create table statements.')
          cb(err)
        }
        else {
          console.log('Finished create table statements.')
          cb(null, connection, table_names, field_names)
        }
      })
    },
    // Write insert into statements
    function write_insertInto(connection, table_names, field_names, cb) {
      write_insert_intos(filename, connection, table_names, field_names, (err) => {
        if (err) {
          console.log('Error at writing insert into statements.')
          cb(err)
        }
        else {
          console.log('Finished insert into statements.')
          cb(null, connection)
        }
      })
    },
    // Close database connection
    function close(connection, cb) {
      connection.end(function(err) {
        if (err) {
          console.log('Error closing connection to databse.')
          cb(err)
        } else {
          console.log('Closed connection to database.')
          cb(null)
        }
      })
    }
  ],
  function (err, result) {
    if (err) {
      callback(err)
    } else {
      callback(null)
    }
  })
}

/*
Selects which function to run from user input using LISTENER.
Returns nothing.
*/
function select(listener) {
  console.log()
  listener.question('dp: What would you like to do?\n\n', (reply)=> {
    console.log()
    select_loop(listener, reply.split(' '), 0)
  })
}


// MAIN ------------------------------------------------------------------------------------------------------------------------------------

const listener = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false
})

select(listener)
