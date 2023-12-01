
docker run -d -v C:/Users/Paqua/OneDrive/Documents/Mastere_ESD/Info1/Projet/:/Data --name delvich-cassandra cassandra

docker exec -it delvich-cassandra bash

CREATE KEYSPACE IF NOT EXISTS exutoire
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use exutoire;

/* QUESTION 1 */
/* Le nombre d'exutoires pour les déchets Dématérialisation */

CREATE TABLE nombre_exutoire(
    label TEXT,
    id_exutoire INT,
    type TEXT,
    description TEXT,
    id_dechet INT,
    nom_dechet TEXT,
    famille_dechet TEXT,
    PRIMARY KEY (label, id_dechet)
);

COPY nombre_exutoire (id_exutoire, label, type, description, id_dechet, nom_dechet, famille_dechet) FROM '/Data/relation_dechets_exutoires.csv' WITH DELIMITER = ';' AND HEADER = TRUE;

SELECT label, COUNT(*) as "Nombre d'exutoire"
FROM nombre_exutoire
WHERE label = 'Dématérialisation';

/* QUESTION 2 */
/* Quel est le type d'exutoire pour les déchets de Peinture à l'eau ? */

CREATE TABLE type_extuoire_par_dechet(
    label TEXT,
    id_exutoire INT,
    type TEXT,
    description TEXT,
    id_dechet INT,
    nom_dechet TEXT,
    famille_dechet TEXT,
    PRIMARY KEY(nom_dechet, type)
);

COPY type_extuoire_par_dechet (id_exutoire, label, type, description, id_dechet, nom_dechet, famille_dechet) FROM '/Data/relation_dechets_exutoires.csv' WITH DELIMITER = ';' AND HEADER = TRUE;


SELECT type, nom_dechet
FROM type_extuoire_par_dechet 
WHERE nom_dechet = 'Peinture à l''eau';

/* QUESTION 3 */
/* Quels sont les déchets associés à l'exutoire id 3 */

CREATE TABLE nom_dechet_par_id_exutoire (
    label TEXT,
    id_exutoire INT,
    type TEXT,
    description TEXT,
    id_dechet INT,
    nom_dechet TEXT,
    famille_dechet TEXT,
    PRIMARY KEY (id_exutoire, id_dechet)
);

COPY nom_dechet_par_id_exutoire (id_exutoire, label, type, description, id_dechet, nom_dechet, famille_dechet) FROM '/Data/relation_dechets_exutoires.csv' WITH DELIMITER = ';' AND HEADER = TRUE;

SELECT id_exutoire, nom_dechet
FROM nom_dechet_par_id_exutoire
WHERE id_exutoire = 3;

/* QUESTION 4 */
/* Quels sont les déchets qui ont comme famille Verre */

CREATE TABLE dechets_par_famille_de_dechet(
    label TEXT,
    id_exutoire INT,
    type TEXT,
    description TEXT,
    id_dechet INT,
    nom_dechet TEXT,
    famille_dechet TEXT,
    PRIMARY KEY (famille_dechet, nom_dechet)
);

COPY dechets_par_famille_de_dechet (id_exutoire, label, type, description, id_dechet, nom_dechet, famille_dechet) FROM '/Data/relation_dechets_exutoires.csv' WITH DELIMITER = ';' AND HEADER = TRUE;


SELECT nom_dechet, famille_dechet
FROM dechets_par_famille_de_dechet
WHERE famille_dechet = 'Verre'; 

/* QUESTION 5 */
/* Quelle est la description de l'exutoire 6 */

SELECT id_exutoire, description
FROM nom_dechet_par_id_exutoire
WHERE id_exutoire = 6
LIMIT 1;

/* QUESTION 6 */
/* Combien de type de déchets sont concernés par les exutoires de type Conseil */

CREATE TABLE famille_dechet_par_exutoire(
    label TEXT,
    id_exutoire INT,
    type TEXT,
    description TEXT,
    id_dechet INT,
    nom_dechet TEXT,
    famille_dechet TEXT,
    PRIMARY KEY (type, famille_dechet)
);

COPY famille_dechet_par_exutoire (id_exutoire, label, type, description, id_dechet, nom_dechet, famille_dechet) FROM '/Data/relation_dechets_exutoires.csv' WITH DELIMITER = ';' AND HEADER = TRUE;

SELECT type, COUNT(famille_dechet) as nombre_famille_dechet
FROM famille_dechet_par_exutoire
WHERE type = 'Conseil';

/* QUESTION 7 */
/* Quel est le type d'exutoire qui contient le plus de déchets différents ? */

CREATE TABLE nom_dechet_par_type_exutoire(
    label TEXT,
    id_exutoire INT,
    type TEXT,
    description TEXT,
    id_dechet INT,
    nom_dechet TEXT,
    famille_dechet TEXT,
    PRIMARY KEY (type, id_dechet, famille_dechet)
    )WITH CLUSTERING ORDER BY (id_dechet DESC);

COPY nom_dechet_par_type_exutoire (id_exutoire, label, type, description, id_dechet, nom_dechet, famille_dechet) FROM '/Data/relation_dechets_exutoires.csv' WITH DELIMITER = ';' AND HEADER = TRUE;


SELECT type, COUNT (id_dechet) AS "Nombre de type de déchet"
FROM nom_dechet_par_type_exutoire
GROUP BY type;

/* QUESTION 8 */
/* Quel est le type d'exutoire le plus courant ? */

SELECT type, COUNT(famille_dechet) as nombre_famille_dechet
FROM famille_dechet_par_exutoire;


/* QUESTION 9 */
/* Quel est le type d'exutoire le plus courant par famille de déchet ? */
CREATE TABLE famille_par_type_exutoire(
    label TEXT,
    id_exutoire INT,
    type TEXT,
    description TEXT,
    id_dechet INT,
    nom_dechet TEXT,
    famille_dechet TEXT,
    PRIMARY KEY ((famille_dechet, type), id_dechet)
);

COPY famille_par_type_exutoire(id_exutoire, label, type, description, id_dechet, nom_dechet, famille_dechet) FROM '/Data/relation_dechets_exutoires.csv' WITH DELIMITER = ';' AND HEADER = TRUE;

SELECT famille_dechet, type, COUNT (id_exutoire) as Nombre
FROM famille_par_type_exutoire
GROUP BY famille_dechet, type;

/* QUESTION 10 */
/* Quels sont les labels d'exutoires associés à chaque famille de déchets ?*/

CREATE TABLE label_par_famille(
    label TEXT,
    id_exutoire INT,
    type TEXT,
    description TEXT,
    id_dechet INT,
    nom_dechet TEXT,
    famille_dechet TEXT,
    PRIMARY KEY (famille_dechet, label)
)WITH CLUSTERING ORDER BY (label ASC);

COPY label_par_famille (id_exutoire, label, type, description, id_dechet, nom_dechet, famille_dechet) FROM '/Data/relation_dechets_exutoires.csv' WITH DELIMITER = ';' AND HEADER = TRUE;

SELECT famille_dechet, label
FROM label_par_famille
WHERE famille_dechet = 'Culture loisirs'; 









SOURCE 'Data/part1.cql'