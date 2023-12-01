
// PARTIE 1
use('ExutoireStore')
db.Exutoire.countDocuments({})


//1-Quel est le nombre d'exutoires pour les déchets Dématérialisation
use('ExutoireStore')
db.Exutoire.countDocuments({
  "label": "Dématérialisation"
});

//2- Quel est le type d'exutoire pour les déchets de Peinture à l'eau ?

use('ExutoireStore')
db.Exutoire.find({
  "nom_dechet": "Peinture à l'eau"
});

//3 -Quels sont les déchets associés à l'exutoire id 3 ?

use('ExutoireStore')
db.Exutoire.find({
    "id_exutoire": 3
}, {
    "_id": 0,
    "id_dechet": 1,
    "nom_dechet": 1
}).toArray();
// 4 - Quels sont les déchets qui ont comme famille Verre

use('ExutoireStore')
db.Exutoire.find({
  "famille_dechet": "Verre"
}, {
  "_id": 0,
  "famille_dechet":1,
  "id_dechet": 1,
  "nom_dechet": 1,
}).toArray();


// 5- Quelle est la description de l'exutoire 6
use('ExutoireStore')
db.Exutoire.findOne({
  "id_exutoire": 6
}, {
  "_id": 0,
  "description": 1
});

//6 - Combien de type de déchets sont concernés par les exutoires de type Conseil ?
use('ExutoireStore')
db.Exutoire.distinct("nom_dechet", {
  "type": "Conseil"
});


// 7- Quel est le type d'exutoire qui contient le plus de déchets différents ?
use('ExutoireStore')
db.Exutoire.aggregate([
  {
      $group: {
          _id: "$type",
          count: { $addToSet: "$nom_dechet" }
      }
  },
  {
      $project: {
          _id: 1,
          count: { $size: "$count" }
      }
  },
  {
      $sort: { count: -1 }
  },
  {
      $limit: 1
  }
]);

//8 - Quel est le type d'exutoire le plus courant ?
use('ExutoireStore')
db.Exutoire.aggregate([
    {
        $group: {
            _id: "$type",
            count: { $sum: 1 }
        }
    },
    {
        $sort: { count: -1 }
    },
    {
        $limit: 1
    }
]);

//9 - Quel est le type d'exutoire le plus courant par famille de déchet ?
use('ExutoireStore')
db.Exutoire.aggregate([
    {
        $group: {
            _id: { type: "$type", famille_dechet: "$famille_dechet" },
            count: { $sum: 1 }
        }
    },
    {
        $sort: { "_id.famille_dechet": 1, count: -1 }
    },
    {
        $group: {
            _id: "$_id.famille_dechet",
            type: { $first: "$_id.type" },
            count: { $first: "$count" }
        }
    },
    {
        $sort: { count: -1 }
    }
]);

// 10- Quels sont les labels d'exutoires associés à chaque famille de déchets ?
use('ExutoireStore')
db.Exutoire.aggregate([
    {
        $group: {
            _id: { famille_dechet: "$famille_dechet" },
            labels: { $addToSet: "$label" }
        }
    }
]);


// 11- Indiquer une clé qui permettrait de s'assurer que les données soient réparties de manière uniforme sur un cluster distribué.
// Il faudra choisir une clé de sharding de cardinalité, c'est-à-dire un grand nombre de valeurs uniques. Cela contribue à répartir uniformément les données.
// Dans notre, il serait intéressant de prendre comme clé de sharding
// 12 - Indiquer une clé qui permettrait de ne traiter les requêtes ne concernant que les déchets sur un seul noeud.






use('AtmosphereStore');
db.Atmosphere.findOne({})

// Combien de documents
use('AtmosphereStore');
db.Atmosphere.countDocuments({})

// 1- Quels sont les communes qui ont eu un indice de qualité de l'air Moyen ?
use('AtmosphereStore');
db.Atmosphere.distinct("properties.lib_zone", {
  "properties.lib_qual": "Moyen",
  "properties.type_zone": "commune"
})

// 2- Quelle est le code de qualité de l'air maximal sur la commune de Saint-Genès-Champanelle
use('AtmosphereStore');
db.Atmosphere.find({"properties.lib_zone": "Saint-Genès-Champanelle"}, 
{"properties.lib_zone":1, "properties.code_qual":1}).sort({ "properties.code_qual": -1 }).limit(1)

// 3- Quelles sont les différentes communes observées ?
use('AtmosphereStore');
// $addToSet permet d'acceder
db.Atmosphere.aggregate([
  {
    $group: {
      _id: null,
      lib_zone_values: { $addToSet: "$properties.lib_zone" },
      code_zone_values: { $addToSet: "$properties.code_zone" }
    }
  },
  {
    $project: {
      _id: 0,
      lib_zone_values: 1,
      autre_champ_values: 1
    }
  }
])

use('AtmosphereStore');
db.Atmosphere.distinct("properties.lib_zone")

// 4- Quelle est le code qualité moyen par commune ?
use('AtmosphereStore');
db.Atmosphere.aggregate([
  {
    $group: {
      _id: "$properties.lib_zone",
      qualite_moyen: { $avg: "$properties.code_qual" } 
    }
  }
])

// 5- Quelle est la qualité de l'air la plus récente dans un rayon de 1 km de la position GPS 45.77441539864761, 3.0890499134717686
use('AtmosphereStore');
db.Atmosphere.createIndex({ "geometry.coordinates": "2dsphere" })
db.Atmosphere.aggregate([
  {
    $geoNear: {
      near: {
        type: "Point",
        coordinates: [3.0890499134717686, 45.77441539864761]
      },
      distanceField: "distance",
      maxDistance: 1000,
      spherical: true
    }
  },
  {
    $sort: {
      "properties.date_ech": -1
    }
  },
  {
    $limit: 1
  },
  {
    $project: {
      _id: 0,
      "properties.code_qual": 1,
      "properties.date_ech": 1,
      "properties.code_zone":1,
      "properties.lib_zone":1
    }
  }
])

// 5- Bis Pour observer la réponse à la question précédente
use('AtmosphereStore');
db.Atmosphere.find({
  $where: "this.properties.code_zone === '63113'"
})


// 6 - Combien y-a t'il de relevé d'indice différent pour les communes dont la lettre commence par C 
use('AtmosphereStore');
db.Atmosphere.aggregate([
  {
    $match: {
      "properties.lib_zone": { $regex: /^C/i }
    }
  },
  {
    $group: {
      _id: "$properties.lib_zone",
      count: { $sum: 1 }
    }
  },
  {
    $group: {
      _id:null,
      Nombre_total: { $sum: 1 }
    }
  }
])

// 7- Pour chaque libellé de qualité de l'air, quelles sont les communes qui lui sont associé ?
use('AtmosphereStore');
db.Atmosphere.aggregate([
  {
    $group: {
      _id: "$properties.lib_qual",
      communes: { $addToSet: "$properties.lib_zone" }
    }
  }
])

// 8- Quelle est la concentration moyenne de PM10 dans la région Auvergne-Rhône-Alpes le 22 octobre 2023 ?
use('AtmosphereStore');
db.Atmosphere.aggregate([
  {
    $match: {
      "properties.epsg_reg": "2154",
      "properties.date_ech": { $gte: "2023-10-22T00:00:00Z", $lt: "2023-10-23T00:00:00Z" }, 
      "properties.code_pm10": { $exists: true }
    }
  },
  {
    $group: {
      _id: null,
      avg_pm10_auv_rhone_alpes: { $avg: "$properties.conc_pm10" }
    }
  }
])

// 9- Afficher l'évolution de la concentration de NO2 dans la commune de Clermont-Ferrand à travers le temps.
use('AtmosphereStore');
db.Atmosphere.find({
  "properties.lib_zone": "Clermont-Ferrand",
  "properties.code_no2": { $exists: true }
}, {
  "_id": 0,
  "properties.date_ech": 1,
  "properties.conc_no2": 1
}).sort({ "properties.date_ech": 1 })

// 10 - Quelle est la commune qui a l'indice de concentration d'ozone le plus faible sur la période observée ?

use('AtmosphereStore');
db.Atmosphere.find({
  "properties.conc_o3": { $exists: true } 
}, {
   "_id":0,
   "properties.lib_zone":1, "properties.conc_o3":1
}).sort({ "properties.conc_o3": 1}).limit(1)

// 11- Quelle serait la clé de sharding à utiliser pour pouvoir s'assurer que tous les enregistrements 
// d'une même commune soient traités par le même noeud ?

// La clé de sharding idéale pour s'assurer que tous les enregistrements d'une même commune soient 
// traités par le même nœud serait le champ "properties.lib_zone" (ou le code postal), qui représente 
// le nom de la commune. En utilisant ce champ comme clé de sharding, MongoDB s'assurera que tous les 
// documents ayant la même  valeur dans le champ "properties.lib_zone" seront stockés sur le même nœud de shard.
