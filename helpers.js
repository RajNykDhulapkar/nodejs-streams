// slugify function to convert a string to a slug for file names
const slugify = (text) => text.toString().toLowerCase()
    .replace(/\s+/g, '-')           // Replace spaces with -
    .replace(/[^\w\-]+/g, '')       // Remove all non-word chars
    .replace(/\-\-+/g, '-')         // Replace multiple - with single -
    .replace(/^-+/, '')             // Trim - from start of text
    .replace(/-+$/, '');            // Trim - from end of text

// zip function to zip two arrays into an object
const zip = (keys, values) =>
    keys.reduce((obj, field, i) => {
        obj[field] = values[i];
        return obj;
    }, {});

// function to unzip an object into an array
const unzip = (obj) =>
    Object.keys(obj).reduce((arr, field) =>
        arr.push(obj[field]) && arr, []);

// function to omit fields from an object
const omit = (obj, fields) => {
    // check if obj is array
    if (Array.isArray(obj)) return obj.filter(field => !fields.includes(field));

    const newObj = {};
    Object.keys(obj).forEach(field => {
        if (!fields.includes(field)) {
            newObj[field] = obj[field];
        }
    });
    return newObj;
};

// function to make Buffer from array
const makeBuf = (data) =>
    Buffer.from(data.join(',') + "\n", 'utf8');

module.exports = {
    slugify,
    zip,
    unzip,
    omit,
    makeBuf
};