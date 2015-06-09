var neo4j = require('neo4j');
var db = new neo4j.GraphDatabase('http://neo4j:waag@localhost:3000');
var crypto = require('crypto')
var async = require('async');
var util = require('util');

var dataSets;

//run project with: node test.js datasets=kloeke,nvb,etc

process.argv.forEach(function(string){
  var split = string.split('=');
  if(split.length > 1 && split[0] === 'datasets'){
    dataSets = split[1].split(',');
    console.log('datasets to take into account: ' + dataSets.join(', '));
  }
});

function hashIds(ids)
{
	ids.sort();
	
	var shasum = crypto.createHash('sha224');
	ids.forEach(function(id){
		shasum.update(id);
	});
	
	return shasum.digest('hex');
}

var visited_nodes = {};

function record_visits(ids, hash){
	ids.forEach(function(id){
		visited_nodes[id] = hash;
	});
}

function klont(n, callback)
{
	var hash = visited_nodes[n];
	if(hash){
		return callback(null, hash);
	}

  return async.waterfall([

		// find all connected nodes
		db.cypher.bind(db, {
		    query: [
		    	'START n=node({start})',
				  'MATCH p = (n:PIT)-[:SAMEHGCONCEPT*0..5]->(m:PIT)',
				  'RETURN DISTINCT m'
		    ].join('\n'),
		    params: {
		        start: n
		    }
		}),
			
		// process results
		function (results, callback) {
      if(!results.length) return callback();

      var node_ids = [],
          hg_ids = [];

      results.forEach(function(r){
        node_ids.push(r.m._id);
        
        if(!dataSets.length || ~dataSets.indexOf(r.m.properties.sourceid)){
          hg_ids.push(r.m.properties.hgid);
        }
      });


			// hash it
			var hash = hg_ids.length ? hashIds(hg_ids) : true;
      
      
      // avoid doing duplicate work
      record_visits(node_ids, hash);
			
			if(hash !== true) console.log(JSON.stringify({hgConceptId: hash, PITs: hg_ids}));
			callback();
		}
	], callback);
}

function init(){
  db.cypher({
    query: [
      'Match (node)',
      'Return node',
      'Order by ID(node) desc',
      'Limit 1'
    ].join('\n')
  }, function(err, results){
    var max = results[0].node._id + 1;

    async.timesSeries(max, klont, function done(err){
      console.log(err || 'yay');
    });
  });
}

init();


