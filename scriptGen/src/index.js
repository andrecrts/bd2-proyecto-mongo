var fs = require('fs');


var normalizedPath = require("path").join(__dirname, "leagues");

let data = "["

require("fs").readdirSync(normalizedPath).forEach(function (file) {
    const { SE, SP } = require("./leagues/" + file);
    const liga = file.split('.')[0];

    const queries = []

    SP.forEach((round, index) => {
        const roundId = `${liga}${index}`
        // queries.push(`insert into round VALUES('${roundId}', '${liga}', '${index}');`)

        round.forEach(match => {
            const e1 = SE[parseInt(match.a1)].split('|')[1];
            const e2 = SE[parseInt(match.a2)].split('|')[1];
            const date = match.d;
            const g1 = match.g1;
            const g2 = match.g2;

            const query = `INSERT INTO partido VALUES('${roundId}', (SELECT  id FROM equipo WHERE name = '${e1}'), (SELECT  id FROM equipo WHERE name = '${e2}'), ${g1}, ${g2}, '${date}');`;

          data += `{ "date": "${date}", "liga":"${liga}", "ronda": ${index}, "equipo1": "${e1}", "equipo2": "${e2}", "goles1":${g1}, "goles2": ${g2}},`

            // queries.push(query)
        })
    })
}
);

data += "]"
    fs.writeFile(__dirname + `/../scripts/data.json`, data, function (err) {
        if (err) throw err;
      console.log(`File for league error: ${err}`);
    });
