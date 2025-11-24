-- SET search_path TO yape_1_000_000;
SET search_path TO yape_100_000;
SET search_path TO yape_10_000;
SET search_path TO yape_1_000;

-- Query 1
-- Indices

DROP INDEX idx_transaccion_fecha_estado;
DROP INDEX idx_operacion_billetera;
DROP INDEX idx_billetera_actor;
DROP INDEX idx_billetera_persona_nivel;

-- Indice
-- CREATE INDEX idx_transaccion_fecha_estado
-- ON Transaccion_Yape(fecha_hora, estado);

-- Índice para JOIN con operaciones
-- CREATE INDEX idx_operacion_billetera
-- ON Operacion_Yape(id_billetera_emisor);

-- Índice compuesto para billetera-actor
-- CREATE INDEX idx_billetera_actor
-- ON Billetera_Yape(id_actor);


EXPLAIN ANALYZE SELECT
    COALESCE(p.dni, e.ruc, 'Externo') AS identificador,
    COALESCE(p.correo, e.razon_social, 'Usuario Externo') AS nombre,
    COUNT(*) AS total_transacciones,
    ROUND(AVG(t.monto), 2) AS monto_promedio,
    ROUND(SUM(t.monto), 2) AS volumen_total
FROM Transaccion_Yape t
JOIN Operacion_Yape o ON t.id_transaccion = o.id_transaccion
JOIN Billetera_Yape b ON o.id_billetera_emisor = b.id_billetera
LEFT JOIN Actor_Yape a ON b.id_actor = a.id_actor
LEFT JOIN Persona p ON a.id_actor = p.id_actor
LEFT JOIN Empresa e ON a.id_actor = e.id_actor
WHERE t.estado = 'Exitosa'
  AND t.fecha_hora >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY identificador, nombre
ORDER BY total_transacciones DESC
LIMIT 10;


-- Query 2
EXPLAIN ANALYZE SELECT
    EXTRACT(HOUR FROM fecha_hora) AS hora,
    COUNT(*) AS total_transacciones,
    ROUND(AVG(monto), 2) AS monto_promedio,
    COUNT(DISTINCT o.id_billetera_emisor) AS usuarios_unicos
FROM Transaccion_Yape t
JOIN Operacion_Yape o ON t.id_transaccion = o.id_transaccion
WHERE t.estado = 'Exitosa'
  AND t.fecha_hora >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY EXTRACT(HOUR FROM fecha_hora)
ORDER BY total_transacciones DESC;

-- Query 3
EXPLAIN ANALYZE SELECT
    CASE
        WHEN bp.id_billetera IS NOT NULL THEN 'Persona'
        WHEN bop.id_billetera IS NOT NULL THEN 'OtherPersona'
        WHEN epn.id_actor IS NOT NULL THEN 'PequenoNegocio'
        WHEN es.id_actor IS NOT NULL THEN 'EmpresaServicios'
        WHEN eae.id_actor IS NOT NULL THEN 'EmpresaAE'
        ELSE 'Externa'
    END AS tipo_billetera,

    COUNT(*) AS total_transacciones,

    SUM(
        CASE WHEN t.estado = 'Exitosa'
        THEN 1 ELSE 0 END
    ) AS exitosas,

    ROUND(AVG(t.monto), 2) AS monto_promedio

FROM Transaccion_Yape t
JOIN Operacion_Yape o
    ON t.id_transaccion = o.id_transaccion
JOIN Billetera_Yape bw
    ON o.id_billetera_emisor = bw.id_billetera

LEFT JOIN Billetera_Persona bp
    ON bw.id_billetera = bp.id_billetera

LEFT JOIN Billetera_Other_Persona bop
    ON bw.id_billetera = bop.id_billetera

LEFT JOIN Billetera_Empresa be
    ON bw.id_billetera = be.id_billetera

LEFT JOIN Empresa_Pequeno_Negocio epn
    ON be.id_billetera IS NOT NULL
   AND epn.id_actor = bw.id_actor

LEFT JOIN Empresa_Servicios es
    ON be.id_billetera IS NOT NULL
   AND es.id_actor = bw.id_actor

LEFT JOIN Empresa_Acceso_Empresarial eae
    ON be.id_billetera IS NOT NULL
   AND eae.id_actor = bw.id_actor

WHERE t.fecha_hora >= CURRENT_DATE - INTERVAL '30 days'

GROUP BY tipo_billetera
ORDER BY total_transacciones DESC;

-- Query 4

-- Indice
-- CREATE INDEX idx_billetera_persona_nivel ON Billetera_Persona(nivel_verificacion, id_billetera);

EXPLAIN ANALYZE SELECT
    bp.nivel_verificacion,
    COUNT(DISTINCT bw.id_billetera) AS total_usuarios,
    COUNT(*) AS total_transacciones,
    ROUND(AVG(t.monto), 2) AS monto_promedio,
    ROUND(
        SUM(CASE WHEN t.estado = 'Exitosa' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS porcentaje_exito
FROM Billetera_Persona bp
JOIN Billetera_Yape bw ON bp.id_billetera = bw.id_billetera
JOIN Operacion_Yape o ON bw.id_billetera = o.id_billetera_emisor
JOIN Transaccion_Yape t ON o.id_transaccion = t.id_transaccion
WHERE t.fecha_hora >= CURRENT_DATE - INTERVAL '60 days'
  AND bp.nivel_verificacion IS NOT NULL
GROUP BY bp.nivel_verificacion
HAVING COUNT(*) >= 5
ORDER BY total_transacciones DESC;
