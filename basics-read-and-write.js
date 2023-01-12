/* 
* Description: Basic example on using the nodejs stream.
* In this example we read data from a csv file and write it to another csv file.
* file: basics-read-and-write.js
*/
const fs = require('fs');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify');

// create a readable stream to read the input CSV file
const readStream = fs.createReadStream('data/large_file.csv');

// create a writable stream to write to the output CSV file
const writeStream = fs.createWriteStream('data/output.csv');

const countries = new Set();

// create a CSV parser
const parser = parse({
    delimiter: ','
});

// create a CSV stringifier
const stringifier = stringify({
    delimiter: ','
});

// pipe the read stream to the CSV parser
readStream.pipe(parser);

// handle the data event
parser.on('data', (data) => {
    // process the data
    // add the country to the set
    countries.add(data[0]);

    // write the data to the output CSV file
    stringifier.write(data);
});

// handle the end event
parser.on('end', () => {
    console.log('Finished reading input CSV file');
    console.log(`Total countries: ${countries.size}`);
    stringifier.end();
});

// handle any errors
parser.on('error', (err) => {
    console.log(err);
});

// pipe the stringifier to the write stream
stringifier.pipe(writeStream);

writeStream.on('finish', () => {
    console.log('Finished writing to output CSV file');
});

writeStream.on('error', (err) => {
    console.log(err);
});
