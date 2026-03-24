import requests
import json
import time
import datetime
from confluent_kafka import Producer

# --- Configuración ---
API_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "gasolineras"

# --- Tiempos de ejecución ---
TIEMPO_ESPERA_EXITO = 3600  # 1 hora (en segundos)
TIEMPO_ESPERA_ERROR = 300   # 5 minutos (en segundos)

def reporte_entrega(err, msg):
    """Callback asíncrono para confirmar la entrega del mensaje a Kafka."""
    if err is not None:
        print(f"❌ Error al entregar el mensaje: {err}")

def obtener_datos_api():
    """Hace la petición GET a la API del Ministerio."""
    hora_actual = time.strftime('%H:%M:%S')
    print(f"\n📡 [{hora_actual}] Consultando la API del Gobierno...")
    try:
        respuesta = requests.get(API_URL, timeout=15)
        if respuesta.status_code == 200:
            return respuesta.json().get("ListaEESSPrecio", [])
        else:
            print(f"❌ Error en la API. Código de estado: {respuesta.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"❌ Error de red al conectar con la API: {e}")
        return []

def main():
    # 1. Configurar el Productor de Confluent Kafka
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'productor-gasolineras-python',
        # Opcional: 'acks': 'all' para máxima fiabilidad de entrega
    }
    productor = Producer(conf)

    print("🟢 Iniciando el servicio productor de gasolineras...")

    # 2. Bucle de ejecución continua
    while True:
        gasolineras = obtener_datos_api()

        # Comprobamos si hubo un error o la lista está vacía
        if not gasolineras:
            print(f"⚠️ No se pudieron obtener datos. Reintentando en {TIEMPO_ESPERA_ERROR // 60} minutos...")
            time.sleep(TIEMPO_ESPERA_ERROR)
            continue

        print(f"🚀 Procesando {len(gasolineras)} registros para Kafka (Topic: '{KAFKA_TOPIC}')...")

        # Capturamos el momento exacto de la consulta (UTC en formato ISO 8601)
        # Lo hacemos fuera del bucle para que todas las gasolineras de esta tanda tengan la misma fecha
        timestamp_consulta = datetime.datetime.now(datetime.UTC).isoformat()

        contador = 0
        for gasolinera in gasolineras:
            # A. Inyectamos nuestro propio timestamp al diccionario
            gasolinera['Fecha Consulta'] = timestamp_consulta
            
            # B. Extraemos el ID único para la Key de Kafka (en bytes)
            id_gasolinera = str(gasolinera["IDEESS"]).encode('utf-8')
            
            # C. Serializamos todo el JSON a bytes
            valor_json = json.dumps(gasolinera).encode('utf-8')
            
            # D. Producimos el mensaje
            productor.produce(
                topic=KAFKA_TOPIC, 
                key=id_gasolinera, 
                value=valor_json, 
                callback=reporte_entrega
            )
            
            # E. Atendemos las llamadas de retorno (callbacks) sin bloquear
            productor.poll(0)
            
            contador += 1
            if contador % 2000 == 0:
                print(f"   -> Encolados {contador} registros...")

        # 3. Forzar la entrega de los mensajes pendientes en el buffer
        print("⏳ Vaciando el buffer interno y confirmando entregas...")
        productor.flush()
        print(f"✅ Descarga y envío completados con éxito ({contador} mensajes).")
        
        # 4. Esperar hasta la próxima actualización programada
        print(f"💤 Esperando {TIEMPO_ESPERA_EXITO // 60} minutos para la próxima consulta...\n")
        time.sleep(TIEMPO_ESPERA_EXITO)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Ejecución detenida manualmente por el usuario. Cerrando el programa de forma segura.")