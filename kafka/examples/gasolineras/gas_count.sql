WITH UltimosPrecios AS (
    SELECT DISTINCT ON (ideess) *
    FROM precios
    ORDER BY ideess, fecha_consulta DESC
),
Conteos AS (
    SELECT 
        COUNT(precio_adblue) AS adblue,
        COUNT(precio_amoniaco) AS amoniaco,
        COUNT(precio_biodiesel) AS biodiesel,
        COUNT(precio_bioetanol) AS bioetanol,
        COUNT(precio_biogas_natural_comprimido) AS biogas_natural_comprimido,
        COUNT(precio_biogas_natural_licuado) AS biogas_natural_licuado,
        COUNT(precio_diesel_renovable) AS diesel_renovable,
        COUNT(precio_gas_natural_comprimido) AS gas_natural_comprimido,
        COUNT(precio_gas_natural_licuado) AS gas_natural_licuado,
        COUNT(precio_gases_licuados_petroleo) AS gases_licuados_petroleo,
        COUNT(precio_gasoleo_a) AS gasoleo_a,
        COUNT(precio_gasoleo_b) AS gasoleo_b,
        COUNT(precio_gasoleo_premium) AS gasoleo_premium,
        COUNT(precio_gasolina_95_e10) AS gasolina_95_e10,
        COUNT(precio_gasolina_95_e25) AS gasolina_95_e25,
        COUNT(precio_gasolina_95_e5) AS gasolina_95_e5,
        COUNT(precio_gasolina_95_e5_premium) AS gasolina_95_e5_premium,
        COUNT(precio_gasolina_95_e85) AS gasolina_95_e85,
        COUNT(precio_gasolina_98_e10) AS gasolina_98_e10,
        COUNT(precio_gasolina_98_e5) AS gasolina_98_e5,
        COUNT(precio_gasolina_renovable) AS gasolina_renovable,
        COUNT(precio_hidrogeno) AS hidrogeno,
        COUNT(precio_metanol) AS metanol
    FROM UltimosPrecios
)
-- Transformamos las columnas en filas (llave y valor)
SELECT key AS tipo_combustible, value::int AS numero_gasolineras
FROM Conteos, jsonb_each(to_jsonb(Conteos.*))
ORDER BY numero_gasolineras DESC;