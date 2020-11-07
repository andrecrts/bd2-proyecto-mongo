mongoimport --jsonArray --db practica --collection movies --file data.json


// query 1
db.partidos.aggregate([
  { $match: { liga: "1819"}},
  { $project: { 
      equipo: "$equipo1", 
      puntos: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
      _id: 0
      }
  },
  { $unionWith: { coll:"partidos",  pipeline: [
    { $match: { liga: "1819"}},
    { $project: { 
        equipo: "$equipo2", 
        puntos: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
        _id: 0
        }
    },
  ]}},
  { $group: { _id: "$equipo", total: { $sum: "$puntos"}}},
  { $project: { _id: 0, equipo: "$_id", total: 1, liga: 1}},
  { $sort: { total: -1}},
])

//query2
db.partidos.aggregate([
  { $project: {
      equipo: "$equipo1",
      puntos: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
      _id: 0,
      liga: 1,
      }
  },
  { $unionWith: { coll:"partidos",  pipeline: [
    { $project: {
        equipo: "$equipo2",
        puntos: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
        _id: 0,
        liga: 1,
        }
    },
  ]}},
  { $group: { _id: { equipo: "$equipo", liga: "$liga"}, total: { $sum: "$puntos"}, liga: {$first: "$liga"} }},
  { $sort: { "_id.liga":-1 , total: -1}},
  { $group: { _id: "$_id.liga", tabla: { $push: "$$ROOT" }}},
  {$project:{tabla:{$slice:["$tabla", 4]}}}
]).pretty()


//query3
db.partidos.aggregate([
  { $project: {
      equipo: "$equipo1",
      puntos: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
      _id: 0,
      liga: 1,
      }
  },
  { $unionWith: { coll:"partidos",  pipeline: [
    { $project: {
        equipo: "$equipo2",
        puntos: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
        _id: 0,
        liga: 1,
        }
    },
  ]}},
  { $group: { _id: { equipo: "$equipo", liga: "$liga"}, total: { $sum: "$puntos"}, liga: {$first: "$liga"} }},
  { $sort: { "_id.liga":-1 , total: -1}},
  { $group: { _id: "$_id.liga", tabla: { $push: "$$ROOT" }}},
  {$project:{tabla:{$slice:["$tabla", 1]}}},
  { $group: { _id: "$tabla._id.equipo", veces: { $sum: 1  }}},
  { $sort: { "veces":-1}},
  { $limit: 5},
]).pretty()


// QUery D
db.partidos.aggregate([
  { $project: { 
      equipo: "$equipo1", 
      puntos: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
      _id: 0,
      liga: 1,
      }
  },
  { $unionWith: { coll:"partidos",  pipeline: [
    { $match: { liga: "9900"}},
    { $project: { 
        equipo: "$equipo2", 
        puntos: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
        _id: 0,
        liga: 1,
        }
    },
  ]}},
  { $match: { liga: "9900"}},
  { $group: { _id: { equipo: "$equipo", liga: "$liga"}, total: { $sum: "$puntos"}, liga: {$first: "$liga"} }},
  { $sort: { "_id.liga":-1 , total: 1}},
  { $group: { _id: "$_id.liga", tabla: { $push: "$$ROOT" }}},
  {$project:{tabla:{$slice:["$tabla", 3]}}},
]).pretty()


// Query E5
db.partidos.aggregate([
  { $project: {
      equipo: "$equipo1",
        rival: "$equipo2",
      victoria: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 1, else: 0 } },
      _id: 0,
      liga: 1,
      }
  },
  { $unionWith: { coll:"partidos",  pipeline: [
    { $project: {
        equipo: "$equipo2",
        rival: "$equipo1",
        victoria: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 1, else: 0 } },
        _id: 0,
        liga: 1,
        }
    },
  ]}},
  { $match: { victoria: 1}},
  { $group: { _id: { equipo: "$equipo", rival: "$rival" }, victorias: { $sum: "$victoria"}}},
  { $project: { equipo: "$_id.equipo", rival:"$_id.rival", victorias: 1, _id: 0}},
  { $sort: { equipo:1, victorias: -1}},
  { $group: { _id: "$equipo", tabla: { $push: "$$ROOT" }}},
  { $addFields: { victima: { $first: "$tabla" } } },
  {$project:{equipo: "$_id", victima: "$victima.rival", veces: "$victima.victorias", _id: 0}},
  { $sort: { veces: -1}},
]).pretty()

//Query F6

db.partidos.aggregate([
  { $project: {
      equipo: "$equipo1",
      puntos: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
      _id: 0,
      liga: 1,
      }
  },
  { $unionWith: { coll:"partidos",  pipeline: [
    { $project: {
        equipo: "$equipo2",
        puntos: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
        _id: 0,
        liga: 1,
        }
    },
  ]}},
  { $group: { _id: { equipo: "$equipo", liga: "$liga"}, total: { $sum: "$puntos"}, liga: {$first: "$liga"} }},
  { $sort: { "_id.liga":-1 , total: -1}},
  { $group: { _id: "$_id.liga", tabla: { $push: "$$ROOT" }}},
  { "$unwind": { "path": "$tabla", includeArrayIndex: "rank"} },
  { $match: { "tabla._id.equipo": "Barcelona"}},
  { $project: { liga: "$_id", rank: { $sum:["$rank",1] }, equipo:"$tabla._id.equipo"}}
]).pretty()


// Query G 7
db.partidos.aggregate([
  { $sort: {goles1: -1, goles2: -1}},
  { $limit: 1},
]).pretty()


//Query H 8
db.partidos.aggregate([
  { $match: { liga: "1011"}},
  { $project: {
      equipo: "$equipo1",
      puntos: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
      _id: 0,
      liga: 1,
      ronda:1,
      }
  },
  { $unionWith: { coll:"partidos",  pipeline: [
  { $match: { liga: "1011"}},
    { $project: {
        equipo: "$equipo2",
        puntos: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
        _id: 0,
        liga: 1,
        ronda:1,
        }
    },
  ]}},
  { $sort: { ronda:1}},
  { $group: { _id: "$equipo",
                'rondas': { '$push': '$ronda' },
                'totals': { '$push': '$puntos' }}},
                 {
            '$unwind': {
                'path' : '$rondas',
                'includeArrayIndex' : 'index'
            }
        },

        {
            '$project': {
                '_id': 0,
                ronda: "$rondas",
                equipo: "$_id",
                'puntos': { '$sum': { '$slice': [ '$totals', { '$add': [ '$index', 1 ] } ] } },
            }
        },
  { $sort: { ronda:1, puntos: -1}},
  { $group: { _id: "$ronda", tabla: { $push: "$$ROOT" }}},
  { $addFields: { campeon: { $first: "$tabla" } } },
  {$project:{ronda: "$_id", campeon: "$campeon.equipo", puntos: "$campeon.puntos", _id: 0}},
  { $sort: { ronda:1}},
]).pretty()

// Query I 9
db.partidos.aggregate([
  { $match: { liga: "0001"}},
  { $project: {
      equipo: "$equipo1",
      puntos: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
      _id: 0,
      liga: 1,
      ronda:1,
      }
  },
  { $unionWith: { coll:"partidos",  pipeline: [
  { $match: { liga: "0001"}},
    { $project: {
        equipo: "$equipo2",
        puntos: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 3, else: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0  }} } },
        _id: 0,
        liga: 1,
        ronda:1,
        }
    },
  ]}},
  { $sort: { ronda:1}},
  { $group: { _id: "$equipo",
                'rondas': { '$push': '$ronda' },
                'totals': { '$push': '$puntos' }}},
                 {
            '$unwind': {
                'path' : '$rondas',
                'includeArrayIndex' : 'index'
            }
        },

        {
            '$project': {
                '_id': 0,
                ronda: "$rondas",
                equipo: "$_id",
                'puntos': { '$sum': { '$slice': [ '$totals', { '$add': [ '$index', 1 ] } ] } },
            }
        },
  { $sort: { ronda:1, puntos: -1}},
  { $group: { _id: "$ronda", tabla: { $push: "$$ROOT" }}},
  { $addFields: { ultimo: { $last: "$tabla" } } },
  {$project:{ronda: "$_id", ultimo: "$ultimo.equipo", puntos: "$ultimo.puntos", _id: 0}},
  { $sort: { ronda:1}},
]).pretty()

//Query J 10
db.partidos.aggregate([
  { $project: {
      equipo: "$equipo1",
      goles: "$goles1",
      _id: 0,
      liga: 1,
      }
  },
  { $unionWith: { coll:"partidos",  pipeline: [
    { $project: {
        equipo: "$equipo2",
        goles: "$goles2",
        _id: 0,
        liga: 1,
        }
    },
  ]}},
  { $group: { _id: "$liga", total: { $sum: "$goles" }, tabla: { $push: "$$ROOT" }}},
  { "$unwind": { "path": "$tabla" } },
  {$project:{liga: "$_id", total: 1, equipo: "$tabla.equipo", goles: "$tabla.goles", _id: 0}},
  { $group: { _id: { total: "$total", liga: "$liga", equipo: "$equipo" }, totalGolesEquipo: { $sum: "$goles"} }},
  {$project:{liga: "$_id.liga", totalLiga: "$_id.total", equipo: "$_id.equipo", totalGolesEquipo: 1, _id: 0}},

  { $sort: { liga: -1, "totalGolesEquipo": -1}},
  { $group: { _id: { liga: "$liga", totalLiga: "$totalLiga"}, tabla: { $push: "$$ROOT" }}},
   { $addFields: { menosGoles: { $last: "$tabla" } } },
   { $addFields: { masGoles: { $first: "$tabla" } } },
   {$project:{liga: "$_id.liga", totalLiga: "$_id.totalLiga", masGoles: 1,menosGoles: 1, _id: 0}},
]).pretty()


//query k 11
db.partidos.aggregate([
  { $project: {
      equipo: "$equipo1",
      victoria: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 1, else: 0 } },
      derrota: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 1, else: 0 } },
      empate: { $cond: { if: { $eq:["$goles2", "$goles1"]}, then: 1, else: 0 } },
      _id: 0,
      liga: 1,
      }
  },
  { $unionWith: { coll:"partidos",  pipeline: [
    { $project: {
        equipo: "$equipo2",
        victoria: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 1, else: 0 } },
        derrota: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 1, else: 0 } },
        empate: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0 } },
        _id: 0,
        liga: 1,
        }
    },
  ]}},
  { $group: { _id: "$equipo" , victorias: { $sum: "$victoria"} , derrotas: { $sum: "$derrota"}, empates: { $sum: "$empate"}}},
  { $sort: { "victorias": -1}},
  { $limit: 1},
  { $unionWith: { coll:"partidos",  pipeline: [
  { $project: {
      equipo: "$equipo1",
      victoria: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 1, else: 0 } },
      derrota: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 1, else: 0 } },
      empate: { $cond: { if: { $eq:["$goles2", "$goles1"]}, then: 1, else: 0 } },
      _id: 0,
      liga: 1,
      }
  },
  { $unionWith: { coll:"partidos",  pipeline: [
    { $project: {
        equipo: "$equipo2",
        victoria: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 1, else: 0 } },
        derrota: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 1, else: 0 } },
        empate: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0 } },
        _id: 0,
        liga: 1,
        }
    },
  ]}},
  { $group: { _id: "$equipo" , victorias: { $sum: "$victoria"} , derrotas: { $sum: "$derrota"}, empates: { $sum: "$empate"}}},
  { $sort: { "derrotas": -1}},
  { $limit: 1},
  ]}},
  { $unionWith: { coll:"partidos",  pipeline: [
  { $project: {
      equipo: "$equipo1",
      victoria: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 1, else: 0 } },
      derrota: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 1, else: 0 } },
      empate: { $cond: { if: { $eq:["$goles2", "$goles1"]}, then: 1, else: 0 } },
      _id: 0,
      liga: 1,
      }
  },
  { $unionWith: { coll:"partidos",  pipeline: [
    { $project: {
        equipo: "$equipo2",
        victoria: { $cond: { if: { $gt:["$goles2", "$goles1"]}, then: 1, else: 0 } },
        derrota: { $cond: { if: { $gt:["$goles1", "$goles2"]}, then: 1, else: 0 } },
        empate: { $cond: { if: { $eq:["$goles1", "$goles2"]}, then: 1, else: 0 } },
        _id: 0,
        liga: 1,
        }
    },
  ]}},
  { $group: { _id: "$equipo" , victorias: { $sum: "$victoria"} , derrotas: { $sum: "$derrota"}, empates: { $sum: "$empate"}}},
  { $sort: { "empates": -1}},
  { $limit: 1},
  ]}},

]).pretty()
