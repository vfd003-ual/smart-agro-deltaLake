import json
import psycopg2
from confluent_kafka import Consumer

# --- Configuración Kafka ---
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "gasolineras"
GRUPO_CONSUMIDOR = "gasolineras-postgres" 

# --- Configuración PostgreSQL ---
DB_HOST = "localhost"
DB_NAME = "gasolineras" 
DB_USER = "postgres"
DB_PASS = "postgres"
DB_PORT = "5432"

def limpiar_numero(valor_str):
    """Convierte strings con comas en float, y campos vacíos en None (NULL en SQL)."""
    if not valor_str or str(valor_str).strip() == "":
        return None
    try:
        return float(str(valor_str).replace(',', '.'))
    except ValueError:
        return None

def main():
    # 1. Conectar a PostgreSQL
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        )
        cursor = conn.cursor()
        print("🟢 Conectado a PostgreSQL con éxito.")
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")
        return

    # 2. Configurar Kafka
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GRUPO_CONSUMIDOR,
        'auto.offset.reset': 'earliest'
    }
    consumidor = Consumer(conf)
    consumidor.subscribe([KAFKA_TOPIC])

    print(f"📡 Escuchando mensajes en el topic '{KAFKA_TOPIC}'...")

    # Consultas SQL
    query_gasolineras = """
        INSERT INTO gasolineras (
            ideess, rotulo, direccion, cp, horario, latitud, longitud, 
            localidad, municipio, provincia, id_municipio, id_provincia, 
            id_ccaa, margen, remision, tipo_venta, porcentaje_bioetanol, porcentaje_ester_metilico
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ideess) DO UPDATE SET 
            rotulo = EXCLUDED.rotulo, direccion = EXCLUDED.direccion, cp = EXCLUDED.cp,
            horario = EXCLUDED.horario, latitud = EXCLUDED.latitud, longitud = EXCLUDED.longitud,
            localidad = EXCLUDED.localidad, municipio = EXCLUDED.municipio, provincia = EXCLUDED.provincia,
            margen = EXCLUDED.margen, tipo_venta = EXCLUDED.tipo_venta,
            porcentaje_bioetanol = EXCLUDED.porcentaje_bioetanol, 
            porcentaje_ester_metilico = EXCLUDED.porcentaje_ester_metilico;
    """

    query_precios = """
        INSERT INTO precios (
            ideess, fecha_consulta, precio_adblue, precio_amoniaco, precio_biodiesel, 
            precio_bioetanol, precio_biogas_natural_comprimido, precio_biogas_natural_licuado, 
            precio_diesel_renovable, precio_gas_natural_comprimido, precio_gas_natural_licuado, 
            precio_gases_licuados_petroleo, precio_gasoleo_a, precio_gasoleo_b, 
            precio_gasoleo_premium, precio_gasolina_95_e10, precio_gasolina_95_e25, 
            precio_gasolina_95_e5, precio_gasolina_95_e5_premium, precio_gasolina_95_e85, 
            precio_gasolina_98_e10, precio_gasolina_98_e5, precio_gasolina_renovable, 
            precio_hidrogeno, precio_metanol
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (ideess, fecha_consulta) DO NOTHING;
    """

    try:
        while True:
            mensaje = consumidor.poll(timeout=1.0)
            if mensaje is None:
                continue
            if mensaje.error():
                print(f"⚠️ Error en Kafka: {mensaje.error()}")
                continue

            # 3. Extraer el JSON
            datos_raw = mensaje.value().decode('utf-8')
            gas = json.loads(datos_raw)

            ideess = gas.get("IDEESS")
            fecha_consulta = gas.get("Fecha Consulta")
            if not ideess or not fecha_consulta:
                continue

            rotulo = gas.get("Rótulo", "DESCONOCIDO")
            localidad = gas.get("Localidad", "Desconocida")
            precio_95 = gas.get("Precio Gasolina 95 E5", "N/D")
            print(f"⛽ Guardando: {rotulo} ({localidad}) | 95 E5: {precio_95} €")

            datos_gasolinera = (
                ideess,
                gas.get("Rótulo"),
                gas.get("Dirección"),
                gas.get("C.P."),
                gas.get("Horario"),
                limpiar_numero(gas.get("Latitud")),
                limpiar_numero(gas.get("Longitud (WGS84)")),
                gas.get("Localidad"),
                gas.get("Municipio"),
                gas.get("Provincia"),
                gas.get("IDMunicipio"),
                gas.get("IDProvincia"),
                gas.get("IDCCAA"),
                gas.get("Margen"),
                gas.get("Remisión"),
                gas.get("Tipo Venta"),
                limpiar_numero(gas.get("% BioEtanol")),
                limpiar_numero(gas.get("% Éster metílico"))
            )

            datos_precios = (
                ideess,
                fecha_consulta,
                limpiar_numero(gas.get("Precio Adblue")),
                limpiar_numero(gas.get("Precio Amoniaco")),
                limpiar_numero(gas.get("Precio Biodiesel")),
                limpiar_numero(gas.get("Precio Bioetanol")),
                limpiar_numero(gas.get("Precio Biogas Natural Comprimido")),
                limpiar_numero(gas.get("Precio Biogas Natural Licuado")),
                limpiar_numero(gas.get("Precio Diésel Renovable")),
                limpiar_numero(gas.get("Precio Gas Natural Comprimido")),
                limpiar_numero(gas.get("Precio Gas Natural Licuado")),
                limpiar_numero(gas.get("Precio Gases licuados del petróleo")),
                limpiar_numero(gas.get("Precio Gasoleo A")),
                limpiar_numero(gas.get("Precio Gasoleo B")),
                limpiar_numero(gas.get("Precio Gasoleo Premium")),
                limpiar_numero(gas.get("Precio Gasolina 95 E10")),
                limpiar_numero(gas.get("Precio Gasolina 95 E25")),
                limpiar_numero(gas.get("Precio Gasolina 95 E5")),
                limpiar_numero(gas.get("Precio Gasolina 95 E5 Premium")),
                limpiar_numero(gas.get("Precio Gasolina 95 E85")),
                limpiar_numero(gas.get("Precio Gasolina 98 E10")),
                limpiar_numero(gas.get("Precio Gasolina 98 E5")),
                limpiar_numero(gas.get("Precio Gasolina Renovable")),
                limpiar_numero(gas.get("Precio Hidrogeno")),
                limpiar_numero(gas.get("Precio Metanol"))
            )

            # 4. Insertar en base de datos (Transacción Atómica)
            try:
                cursor.execute(query_gasolineras, datos_gasolinera)
                cursor.execute(query_precios, datos_precios)
                conn.commit()
            except Exception as e:
                print(f"❌ Error insertando IDEESS {ideess}: {e}")
                conn.rollback() 

    except KeyboardInterrupt:
        print("\n🛑 Deteniendo el consumidor de forma segura...")
    finally:
        consumidor.close()
        cursor.close()
        conn.close()
        print("✅ Conexiones cerradas.")

if __name__ == '__main__':
    main()