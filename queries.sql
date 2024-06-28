CREATE OR REPLACE FUNCTION datediff(type VARCHAR, date_from DATE, date_to DATE) RETURNS INTEGER LANGUAGE plpgsql
AS
$$
DECLARE age INTERVAL;
BEGIN
    CASE type
        WHEN 'year' THEN
            RETURN date_part('year', date_to) - date_part('year', date_from);
        WHEN 'month' THEN
            age := age(date_to, date_from);
            RETURN date_part('year', age) * 12 + date_part('month', age);
        WHEN 'day' THEN
            RETURN (date_from - date_to)::int;
    END CASE;
END;
$$;

-- 1. Listar los usuarios que realizaron transacciones con tarjeta de crédito
SELECT u.clave_uniforme, u.nombre, u.apellido
FROM Usuario u INNER JOIN Transaccion t ON (u.clave_uniforme = t.CU_Origen)
WHERE t.es_con_tarjeta = TRUE;


-- 2. Listar las transacciones realizadas por un usuario en particular en 2024.
-- Seleccionamos un ususario random de transacciones
SELECT *
FROM Transaccion t
WHERE EXTRACT(YEAR FROM t.fecha) = 2024
AND t.CU_Origen = (SELECT CU_Origen FROM Transaccion WHERE CU_Origen IS NOT NULL LIMIT 1);

-- 3. Obtener los rendimientos en un periodo de tiempo para todos los usuarios.
SELECT 
    ru.clave_uniforme, 
    SUM(r.monto * r.TNA * datediff('day', r.fin_plazo::date, r.comienzo_plazo::date) /  365) AS rendimiento_total_mes_actual 
FROM Rendimiento r INNER  JOIN RendimientoUsuario ru ON r.id = ru.id 
WHERE EXTRACT(MONTH  FROM r.fecha_pago) = 5 AND EXTRACT(YEAR FROM r.fecha_pago) = 2024
GROUP BY ru.clave_uniforme;


-- 4. Cantidad de transacciones rechazadas y la suma de los montos de las mismas para cada usuario.
SELECT 
	u.clave_uniforme, 
	COUNT(*) AS cantidad, 
	SUM(t.monto) AS monto_total
FROM Transaccion t RIGHT JOIN Usuario u ON (t.CU_origen = u.clave_uniforme)
WHERE t.estado = 'Rechazada'
GROUP BY u.clave_uniforme
HAVING SUM(t.monto) IS NOT NULL;


-- 5. El provedor de servicios con mas pagos recibidos en el último mes.
SELECT ps.clave_uniforme, ps.nombre_empresa, COUNT(*) AS pagos_recibidos, SUM(t.monto) AS monto_total
FROM ProveedorServicio ps INNER JOIN Transaccion t ON (ps.clave_uniforme = t.CU_Destino)
WHERE t.estado = 'Completada'
GROUP BY ps.clave_uniforme, ps.nombre_empresa
ORDER BY pagos_recibidos
LIMIT 1;


-- 6. Calcular la diferencia porcentual de rendimientos respecto al mes anterior para cada usuario.
WITH rendimientos_mes_actual AS ( 
	SELECT 
		ru.clave_uniforme, 
		SUM(r.monto * r.TNA *  datediff('day', r.fin_plazo::date, r.comienzo_plazo::date) /  365) AS rendimiento_total_mes_actual 
	FROM Rendimiento r INNER  JOIN RendimientoUsuario ru ON r.id = ru.id 
	WHERE  EXTRACT(MONTH  FROM r.fecha_pago) =  EXTRACT(MONTH  FROM  CURRENT_DATE) 
	GROUP  BY ru.clave_uniforme 
),
rendimientos_mes_anterior AS ( 
	SELECT 
		ru.clave_uniforme, 
		SUM(r.monto * r.TNA *  datediff('day', r.fin_plazo::date, r.comienzo_plazo::date) /  365) AS rendimiento_total_mes_anterior 
	FROM Rendimiento r INNER  JOIN RendimientoUsuario ru ON r.id = ru.id 
	WHERE  EXTRACT(MONTH  FROM r.fecha_pago) =  EXTRACT(MONTH  FROM  CURRENT_DATE) -  1  
	GROUP  BY ru.clave_uniforme 
) 
SELECT 
	rma.clave_uniforme, 
	rma.rendimiento_total_mes_actual, 
	COALESCE(
        -- rma.rendimiento_total_mes_actual / rmb.rendimiento_total_mes_anterior, division por cero
        CASE 
            WHEN rmb.rendimiento_total_mes_anterior = 0 THEN NULL
            ELSE rma.rendimiento_total_mes_actual / rmb.rendimiento_total_mes_anterior
        END,
        -- rma.rendimiento_total_mes_actual / rma.rendimiento_total_mes_actual,
        CASE 
            WHEN rmb.rendimiento_total_mes_anterior = 0 THEN 1
            ELSE 0
        END,
        -- -rmb.rendimiento_total_mes_anterior / rmb.rendimiento_total_mes_anterior, 
        CASE 
            WHEN rma.rendimiento_total_mes_actual = 0 THEN -1
            ELSE 0
        END,
        0) AS porcentaje_cambio 
FROM rendimientos_mes_actual rma LEFT  JOIN rendimientos_mes_anterior rmb ON rma.clave_uniforme = rmb.clave_uniforme;


-- 7. Ranking de usuarios con mayor monto transaccionado en el último mes.
SELECT 
	t.CU_Origen, 
	MAX(t.monto) AS mayor_monto 
FROM Transaccion t
WHERE EXTRACT(MONTH  FROM t.fecha) =  EXTRACT(MONTH  FROM  CURRENT_DATE)
AND t.estado = 'Completada'
GROUP BY t.CU_Origen
ORDER  BY mayor_monto DESC;


-- 8. Ranking de usuarios por rendimientos obtenidos.
SELECT 
	ru.clave_uniforme, 
	SUM(r.monto * r.TNA *  datediff('day', r.fin_plazo::date, r.comienzo_plazo::date) /  365) AS rendimiento_total 
FROM Rendimiento r INNER JOIN RendimientoUsuario ru ON r.id = ru.id 
WHERE r.fin_plazo < CURRENT_DATE
GROUP  BY ru.clave_uniforme 
ORDER  BY rendimiento_total DESC;


-- 9. Calcular el saldo resultante después de cada transaccion
-- Calcular el saldo resultante después de cada transacción recibida
SELECT 
    t.codigo AS operacion_id,
    t.CU_Destino AS usuario_id,
    t.fecha,
    'Transaccion Recibida' AS tipo_operacion,
    t.monto,
    t.monto + SUM(t2.monto) OVER (PARTITION BY t.CU_Destino ORDER BY t.fecha, t.codigo ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS saldo_resultante
FROM Transaccion t
LEFT JOIN Transaccion t2 ON t.CU_Destino = t2.CU_Destino AND t.fecha >= t2.fecha AND t.codigo >= t2.codigo;

-- Calcular el saldo resultante después de cada transacción realizada
SELECT 
    t.codigo AS operacion_id,
    t.CU_Origen AS usuario_id,
    t.fecha,
    'Transaccion Realizada' AS tipo_operacion,
    t.monto * -1 AS monto, -- Se multiplica por -1 para reflejar la disminución del saldo
    (t.monto * -1) + SUM(t2.monto) OVER (PARTITION BY t.CU_Origen ORDER BY t.fecha, t.codigo ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS saldo_resultante
FROM Transaccion t
LEFT JOIN Transaccion t2 ON t.CU_Origen = t2.CU_Origen AND t.fecha >= t2.fecha AND t.codigo >= t2.codigo;


-- 10. Calcular los intereses ganados en transacciones pagadas con tarjeta en el último mes.
SELECT SUM(t.monto * t.interes)
FROM Transaccion t
WHERE t.es_con_tarjeta = TRUE
AND t.fecha >= CURRENT_DATE - INTERVAL '1 month';
