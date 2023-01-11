const fs = require('fs');
const parse = require('csv-parse');

// create a readable stream to read the CSV file
const readStream = fs.createReadStream('large_file.csv');

// create a CSV parser
const parser = parse({
    delimiter: ','
});

// handle the data event
parser.on('data', (data) => {
    console.log(data);
    // do something with the data, e.g. insert into a database
});

// handle the end event
parser.on('end', () => {
    console.log('Finished reading CSV file');
});

// handle any errors
parser.on('error', (err) => {
    console.log(err);
});

// pipe the read stream to the CSV parser
readStream.pipe(parser);
