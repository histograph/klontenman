var neo4j = require('neo4j');
var db = new neo4j.GraphDatabase('http://neo4j:waag@localhost:7474');
var crypto = require('crypto')
var async = require('async');

// var conf = require(process.env.HISTOGRAPH_CONFIG).core.neo4j

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
		

	async.waterfall([

		// find all connected nodes
		db.cypher.bind(db, {
			    query: [
			    	'START n=node({start})',
					'MATCH p = (n)-[*0..5]->(m)',
					'RETURN DISTINCT m'
			    ],
			    params: {
			        start: n
			    },
			}),
			
		// process results
		function (results, callback) {

			// extract the Neo4J node id's
			var node_ids = results.map(function(r){
				return r.m._id;
			});
			
			// extract HG id's
			// TODO actually map hgIds, not name
			var hg_ids = results.map(function(r){
				return r.m.properties.name;
			});
			
			// hash it
			var hash = hashIds(hg_ids);
			
			// avoid doing duplicate work
			record_visits(node_ids, hash);
			
			console.log(JSON.stringify({hgConceptId: hash, PITs: hg_ids}));
			callback(null, hash);
		}
	], callback);
}

async.mapSeries([0,1,2,3], klont);


