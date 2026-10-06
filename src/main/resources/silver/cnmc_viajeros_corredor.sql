-- Silver cnmc_viajeros_corredor: viajeros y plazas ofertadas trimestrales de larga distancia alta velocidad por corredor y
-- empresa (CNMC). Conserva todos los corredores; el del proyecto es Madrid-Barcelona. Cubre lo mismo que cnmc_trimestral
-- (viajeros y plazas) pero en millones y sin la fila `Total`; el total del corredor coincide con el de cnmc_trimestral.
-- Entrada: la vista `entrada` = última foto de l2/cnmc_viajeros_corredor (todo string). Grano: (anio, trimestre, corredor, empresa).
-- Millones con coma decimal ('0,978'), como en cnmc_viajeros_producto. Avlo va dentro de Renfe Viajeros (la CNMC no la separa).
WITH base AS (
    SELECT
        try_cast(substr(`Trimestre`, 1, 4) AS INT)                                                         AS anio,
        try_cast(substr(`Trimestre`, 6, 1) AS INT)                                                         AS trimestre,
        trim(`Corredor`)                                                                                   AS corredor,
        trim(`Empresa`)                                                                                    AS empresa,
        try_cast(regexp_replace(NULLIF(trim(`Viajeros (Mill.)`), ''), ',', '.') AS DECIMAL(14, 4))         AS viajeros_millones,
        try_cast(regexp_replace(NULLIF(trim(`Plazas Ofertadas (Mill.)`), ''), ',', '.') AS DECIMAL(14, 4)) AS plazas_millones,
        _source_file
    FROM entrada
),
mapeada AS (
    SELECT *,
        CASE empresa
            WHEN 'Renfe Viajeros' THEN 'RENFE'
            WHEN 'Iryo'           THEN 'IRYO'
            WHEN 'OUIGO'          THEN 'OUIGO'
            ELSE 'OTRO'
        END AS operador_id
    FROM base
),
deduplicada AS (
    SELECT *, row_number() OVER (PARTITION BY anio, trimestre, corredor, empresa ORDER BY _source_file DESC) AS rn
    FROM mapeada
)
SELECT
    anio,
    trimestre,
    CASE WHEN trimestre BETWEEN 1 AND 4 THEN make_date(anio, (trimestre - 1) * 3 + 1, 1) END AS fecha_inicio,
    corredor,
    empresa,
    operador_id,
    viajeros_millones,
    plazas_millones
FROM deduplicada
WHERE rn = 1
ORDER BY anio, trimestre, corredor, empresa
