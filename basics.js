// Description: Basic example on using the nodejs stream 
// file: basics.js

const fs = require('fs');
const { parse } = require('csv-parse');


// create a readable stream to read the CSV file
const filePath = 'data/large_file.csv';
const readStream = fs.createReadStream(filePath);

// read the size of the file
const stats = fs.statSync(filePath);
const fileSizeInBytes = stats.size;
console.log(`File Path: ${filePath}`);
console.log(`File size: ${fileSizeInBytes} bytes`);

let totalRows = 0;

// create a CSV parser
const parser = parse({
    delimiter: ','
});

// handle the data event
parser.on('data', (data) => {
    // console.log(data);
    totalRows += 1;
    // do something with the data, e.g. insert into a database
});

// handle the end event
parser.on('end', () => {
    console.log('Finished reading CSV file');
    console.log(`Total rows: ${totalRows}`);
});

// handle any errors
parser.on('error', (err) => {
    console.log(err);
});

// pipe the read stream to the CSV parser
readStream.pipe(parser);
