'use strict';

const Fs  = require('fs');
const Csv = require('csv');
const _ = require('lodash');

// le chemin pour le fichier d'origine
let input  = './input.csv';
// le chemin et le nom de fichier output
let output = './data_output.csv';

//reading stream (la lecture des fichier en JS)
let readStream  = Fs.createReadStream(input);
let writeStream = Fs.createWriteStream(output);

// option du fichier input (option du csv)
let opt  = {delimiter: ';', escape: '"', relax: true, skip_empty_lines: true};

let parser = Csv.parse(opt);

// le nombre de ligne du fichier output (commencer par 0)
let numberOfLines = 0;

// clean du fichier d'entrée
let cleaner = Csv.transform(data => {
    // la liste des user agents a supprimer
    const listOfUnWantedUserAgents = ["offline browser", "E-Mail Harvester", "Link Checker", undefined, null];
    // la liste des duration a supprimer
    const listOfUnWantedDurations= ["", " ", "00:00:00", "00:00", "00", null, undefined];
    // la liste des URL a supprimer
    const listOfUnWantedURL= ["", null, undefined];

    // un cas special, il y'a des duration avec plusieurs espaces, on les remplaces avec ""
    if(data[6].match(/\s+/g)) data[6] = ""; // Remplacer la colonne duration qui a que des espaces  "     "  par une ""

    if(
        !_.includes(data[3], listOfUnWantedUserAgents[0]) && // UserAgent de la ligne doit etre différent de offline browser
        !_.includes(data[3], listOfUnWantedUserAgents[1]) && // UserAgent de la ligne doit etre différent de E-Mail Harvester
        !_.includes(data[3], listOfUnWantedUserAgents[2]) && // UserAgent de la ligne doit etre différent de Link Checker
        data[3] !== listOfUnWantedUserAgents[3] && // UserAgent de la ligne doit etre différent de undefined
        data[3] !== listOfUnWantedUserAgents[4] && // UserAgent de la ligne doit etre différent de null
        data[6] != listOfUnWantedDurations[0] && // Duration de la ligne doit etre différent de ""
        data[6] != listOfUnWantedDurations[1] && // Duration de la ligne doit etre différent de " "
        !_.includes(data[6], listOfUnWantedDurations[2]) && // Duration de la ligne doit etre différent de 00:00:00
        data[6] != listOfUnWantedDurations[3] && // Duration de la ligne doit etre différent de 00:00
        data[6] != listOfUnWantedDurations[4] && // Duration de la ligne doit etre différent de 00
        data[6] !== listOfUnWantedDurations[5] && // Duration de la ligne doit etre différent de null
        data[6] !== listOfUnWantedDurations[6] && // Duration de la ligne doit etre différent de undefined
        data[9] !== listOfUnWantedURL[0] && // URL de la ligne doit etre différent de ""
        data[9] !== listOfUnWantedURL[1] && // URL de la ligne doit etre différent de undefined
        data[9] !== listOfUnWantedURL[2] // URL de la ligne doit etre différent de null

    ) {
        console.log("on est à la ligne :", numberOfLines++);
        return data;
    }
});

let stringifier = Csv.stringify({delimiter: ";"});

readStream.pipe(parser).pipe(cleaner).pipe(stringifier).pipe(writeStream);
