/*
*  Description: This script reads a large CSV file and splits it into multiple CSV files
*  based on the country name. It uses the csv-parse and csv-stringify modules to parse
*  and stringify the CSV data.
*  file: basics-data-pipeline.js
*/

const fs = require('fs');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify');

const countryData = {}; // Object to store data for each country

// create a readable stream to read the input CSV file
const readStream = fs.createReadStream('data/large_file.csv');
const fields = ['Country', 'ISO 3166-1 alpha-3', 'Year', 'Total', 'Coal', 'Oil', 'Gas',
    'Cement', 'Flaring', 'Other', 'Per Capita'];
const newFields = fields.filter(field => field !== 'Country' && field !== 'ISO 3166-1 alpha-3');

// create a CSV parser
const parser = parse({
    delimiter: ',',
    headers: fields
});

// slugify function to convert a string to a slug for file names
const slugify = (text) => text.toString().toLowerCase()
    .replace(/\s+/g, '-')           // Replace spaces with -
    .replace(/[^\w\-]+/g, '')       // Remove all non-word chars
    .replace(/\-\-+/g, '-')         // Replace multiple - with single -
    .replace(/^-+/, '')             // Trim - from start of text
    .replace(/-+$/, '');            // Trim - from end of text

// pipe the read stream to the CSV parser
readStream.pipe(parser);

// handle the data event
parser.on('data', (data) => {
    // zip data array and fields array into an object
    data = fields.reduce((obj, field, i) => {
        obj[field] = data[i];
        return obj;
    }, {});

    const country = data.Country;

    if (!countryData[country]) {
        countryData[country] = {
            country,
            stringifier: stringify({
                header: true,
                delimiter: ',',
                columns: newFields,
            }).pipe(fs.createWriteStream(`data/countries/${slugify(country)}.csv`))
        }
        countryData[country].stringifier.write(Buffer.from(newFields.join(',') + "\n", 'utf8'))
    }
    // delete Country field and ISO 3166-1 alpha-3 field from data object
    delete data.Country;
    delete data['ISO 3166-1 alpha-3'];

    // unzip data object into an array
    data = newFields.reduce((arr, field) => arr.push(data[field]) && arr, []);

    countryData[country].stringifier.write(Buffer.from(data.join(',') + "\n", 'utf8'));
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