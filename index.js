const app = require('express')();
const { Client } = require('pg');
const crypto = require('crypto');
const HashRing = require('hashring');
const { url } = require('inspector');

const hashRing = new HashRing();
hashRing.add('5433');
hashRing.add('5434');
hashRing.add('5435');

const clients = {
	'5433': new Client({
		'host': 'localhost',
		'port': '5433',
		'user': 'postgres',
		'password': 'postgres',
		'database': 'postgres',
	}),
	'5434': new Client({
		'host': 'localhost',
		'port': '5434',
		'user': 'postgres',
		'password': 'postgres',
		'database': 'postgres',
	}),
	'5435': new Client({
		'host': 'localhost',
		'port': '5435',
		'user': 'postgres',
		'password': 'postgres',
		'database': 'postgres',
	}),

}
connect();

async function connect() {

	clients['5433'].connect();
	clients['5434'].connect();
	clients['5435'].connect();
}

app.get('/:urlId', async (req, res) => {
	const urlId = req.params.urlId;
	const shard = hashRing.get(urlId);
	const database = clients[shard];
	const result = await database.query('select * from urls where url_id=$1', [urlId]);
	if (result.rowCount > 0) {
		res.send({
			url: result.rows[0],
			shard
		})
	} else {
		res.send({
			error: 'url_not_found'
		})
	}
});

app.post('/', async (req, res) => {
	const url = req.query.url;
	// consistent hashing
	const hash = crypto.createHash('sha256').update(url).digest('base64');
	const urlId = hash.substring(0, 5);
	const shard = hashRing.get(urlId);
	const database = clients[shard];
	await database.query("insert into urls (url, url_id) values($1, $2)", [url, urlId]);
	res.send({
		url,
		urlId,
		shard
	});
});

app.listen(8080, () => {
	console.log('Listening at 8080');
});

