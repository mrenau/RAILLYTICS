-- Silver cnmc_viajeros_producto: viajeros y plazas ofertadas trimestrales por tipo de producto de la CNMC (Cercanías, Media
-- Distancia y Larga Distancia, convencional y alta velocidad), total nacional. NO es el corredor: sirve de contexto del mercado.
-- Entrada: la vista `entrada` = última foto de l2/cnmc_viajeros_producto (todo string). Grano: (anio, trimestre, tipo_producto).
-- La CNMC publica en MILLONES con coma decimal ('108,4298'): se conservan en millones (DECIMAL) y no se convierten a unidades,
-- que aparentaría una precisión de personas que el dato no tiene. Antes de 2018 no hay plazas (NULL).
WITH base AS (
    SELECT
        try_cast(substr(`Trimestre`, 1, 4) AS INT)                                                         AS anio,
        try_cast(substr(`Trimestre`, 6, 1) AS INT)                                                         AS trimestre,
        trim(`Tipo de producto`)                                                                           AS tipo_producto,
        try_cast(regexp_replace(NULLIF(trim(`Viajeros (Mill.)`), ''), ',', '.') AS DECIMAL(14, 4))         AS viajeros_millones,
        try_cast(regexp_replace(NULLIF(trim(`Plazas Ofertadas (Mill.)`), ''), ',', '.') AS DECIMAL(14, 4)) AS plazas_millones,
        _source_file
    FROM entrada
),
deduplicada AS (
    SELECT *, row_number() OVER (PARTITION BY anio, trimestre, tipo_producto ORDER BY _source_file DESC) AS rn
    FROM base
)
SELECT
    anio,
    trimestre,
    CASE WHEN trimestre BETWEEN 1 AND 4 THEN make_date(anio, (trimestre - 1) * 3 + 1, 1) END AS fecha_inicio,
    tipo_producto,
    viajeros_millones,
    plazas_millones
FROM deduplicada
WHERE rn = 1
ORDER BY anio, trimestre, tipo_producto
