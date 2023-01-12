/*
*  Description: This script reads a large CSV file and splits it into multiple CSV files
*  based on the country name. It uses the csv-parse and csv-stringify modules to parse
*  and stringify the CSV data.
*  file: basics-data-pipeline.js
*/
const fs = require('fs');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify');
const { unzip, slugify, zip, omit, makeBuf } = require('./helpers');

const countryData = {}; // Object to store data for each country

// create a readable stream to read the input CSV file
const readStream = fs.createReadStream('data/large_file.csv');
const fields = ['Country', 'ISO 3166-1 alpha-3', 'Year', 'Total',
    'Coal', 'Oil', 'Gas', 'Cement', 'Flaring', 'Other', 'Per Capita'];
const newFields = omit(fields, ['Country', 'ISO 3166-1 alpha-3']);

// create a CSV parser
const parser = parse({
    delimiter: ',',
    headers: fields
});

// pipe the read stream to the CSV parser
readStream.pipe(parser);

// handle the data event
parser.on('data', (data) => {
    // zip data array and fields array into an object
    data = zip(fields, data);

    const country = data.Country;

    if (!countryData[country]) {
        const path = `data/countries/${slugify(country)}.csv`;
        countryData[country] = {
            stringifier: stringify({
                header: true,
                delimiter: ',',
                columns: newFields,
            }).pipe(fs.createWriteStream(path))
        }
        countryData[country].stringifier.write(makeBuf(newFields))
    }
    // delete Country field and ISO 3166-1 alpha-3 field from data object
    data = omit(data, ['Country', 'ISO 3166-1 alpha-3'])

    // unzip data object into an array
    data = unzip(data);
    // write data to the writable stream
    countryData[country].stringifier.write(makeBuf(data));
});

// handle the end event
parser.on('end', () => {
    // end the writable stream
    Object.keys(countryData).forEach(country => {
        countryData[country].stringifier.end();
    });
    console.log('Finished reading input CSV file');
});

// handle any errors
parser.on('error', (err) => console.log(err));