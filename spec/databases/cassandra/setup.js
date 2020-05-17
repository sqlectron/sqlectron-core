import fs from 'fs';
import path from 'path';
import cassandraDriver from 'cassandra-driver';


export default function run(config) {
  before(async () => {
    const client = new cassandraDriver.Client({
      contactPoints: [config.host],
    });
    const script = fs.readFileSync(path.join(__dirname, 'schema/schema.cql'), { encoding: 'utf8' });
    const queries = script.split(';').filter((query) => query.trim().length);
    const promises = [];
    queries.forEach((query) => {
      promises.push(executeQuery(client, query));
    });
    await Promise.all(promises);
  });
}


function executeQuery(client, query) {
  return new Promise((resolve, reject) => {
    client.execute(query, (err, data) => {
      if (err) {
        return reject(err);
      }

      return resolve(data);
    });
  });
}
