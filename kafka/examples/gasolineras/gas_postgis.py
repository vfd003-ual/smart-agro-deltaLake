import psycopg2

# --- Configuración PostgreSQL ---
DB_HOST = "localhost"
DB_NAME = "gasolineras"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_PORT = "5432"

# --- Parámetros de Búsqueda (Ej: La Cañada, Almería) ---
MI_LATITUD = 36.8425
MI_LONGITUD = -2.4041
RADIO_KM = 20
# Pon aquí el nombre exacto de la columna de tu tabla 'precios'.
# Ejemplos: precio_gasolina_95_e5, precio_gasoleo_a, precio_gasolina_98_e5
# Podéis usar la query gas_count.sql para ver el número de gasolineras que tienen cada combustible en España.
COMBUSTIBLE = "precio_gasolina_95_e5" 

def main():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        )
        cursor = conn.cursor()
        
        # Al poner la 'f' delante de las comillas triples, podemos inyectar {COMBUSTIBLE}
        query = f"""
            WITH ultimos_precios AS (
                SELECT DISTINCT ON (ideess) 
                       ideess, {COMBUSTIBLE}, fecha_consulta
                FROM precios
                WHERE {COMBUSTIBLE} IS NOT NULL
                ORDER BY ideess, fecha_consulta DESC
            )
            SELECT 
                g.rotulo, 
                g.direccion, 
                g.localidad, 
                g.latitud,
                g.longitud,
                up.{COMBUSTIBLE},
                ST_Distance(
                    ST_MakePoint(g.longitud, g.latitud)::geography,
                    ST_MakePoint(%s, %s)::geography
                ) / 1000 AS distancia_km
            FROM gasolineras g
            JOIN ultimos_precios up ON g.ideess = up.ideess
            WHERE g.latitud IS NOT NULL AND g.longitud IS NOT NULL
              AND ST_DWithin(
                  ST_MakePoint(g.longitud, g.latitud)::geography,
                  ST_MakePoint(%s, %s)::geography,
                  %s * 1000
              )
            ORDER BY up.{COMBUSTIBLE} ASC, distancia_km ASC
            LIMIT 10;
        """

        parametros = (MI_LONGITUD, MI_LATITUD, MI_LONGITUD, MI_LATITUD, RADIO_KM)
        
        nombre_combustible = COMBUSTIBLE.replace("precio_", "").replace("_", " ").capitalize()
        print(f"\n📍 Top 10 gasolineras a menos de {RADIO_KM} km buscando {nombre_combustible}:\n")        
        
        cursor.execute(query, parametros)
        resultados = cursor.fetchall()

        if not resultados:
            print("No se han encontrado gasolineras con precio en ese radio.")
        else:
            for i, fila in enumerate(resultados):
                rotulo = fila[0].strip() if fila[0] else "Desconocido"
                direccion = fila[1].strip() if fila[1] else "Dirección no disponible"
                localidad = fila[2].strip() if fila[2] else "Localidad no disponible"
                lat = fila[3]
                lon = fila[4]
                precio = fila[5]
                distancia = fila[6]
                enlace_maps = f"https://www.google.com/maps?q={lat},{lon}"
                
                # Asignar icono según posición
                if i == 0:
                    icono = "🥇"
                elif i == 1:
                    icono = "🥈"
                elif i == 2:
                    icono = "🥉"
                else:
                    icono = "⛽"
                
                print(f"{icono} {i+1}. {rotulo} | 💰 {precio:.3f} € | 📏 {distancia:.2f} km")
                print(f"   📍 {direccion} ({localidad}) | 🗺️ {enlace_maps}")
                print()
                
    except Exception as e:
        print(f"❌ Error en la base de datos: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    main()