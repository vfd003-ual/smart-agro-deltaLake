import json, time
import requests
from sseclient import SSEClient
from confluent_kafka import Producer

KAFKA_BROKER = 'localhost:9092' 
KAFKA_TOPIC = 'wikipedia_edits'
WIKI_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'


def delivery_report(err, msg):
    if err is not None:
        print(f"ERROR: Fallo al entregar mensaje a Kafka: {err}")
    # else:
    #     print(f"OK: Mensaje entregado a {msg.topic()} [Partición: {msg.partition()}]")

def run_producer():
    print(f"Conectando al broker de Kafka en {KAFKA_BROKER}...")
    
    conf = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(**conf)
    
    print(f"Conectando al stream de la Wikipedia: {WIKI_STREAM_URL}")

    # Wikipedia requiere User-Agent en todas las conexiones
    headers = {
        'User-Agent': 'InfraestructuraBigData/2026', 
        'Accept': 'text/event-stream'
    }

    
    while True:
        response = requests.get(WIKI_STREAM_URL, stream=True, headers=headers)
        response.raise_for_status()
        sse_client = SSEClient(response)
        print("¡Conexión establecida! Recibiendo datos...")

        try:
            for event in sse_client.events():
                if event.event == 'message':
                    try:
                        edit_data = json.loads(event.data)
                        # Convertimos el diccionario a JSON y luego lo codificamos a utf-8.
                        data_bytes = json.dumps(edit_data).encode('utf-8')           

                        # Extraemos datos para el print en consola
                        user = edit_data.get('user', 'Desconocido')
                        title = edit_data.get('title', 'Sin título')
                        wiki = edit_data.get('wiki', 'Desconocida')
                        is_bot = edit_data.get('bot', False)
                        user_type = "🤖" if is_bot else "🧑"
                        print(f"{user_type} {user} editó '{title}' en {wiki}")


                        producer.produce(KAFKA_TOPIC, key=wiki, value=data_bytes, callback=delivery_report)
                        producer.poll(0) # Procesa los callbacks pendientes de los mensajes anteriores sin bloquear la ejecución.

                    except json.JSONDecodeError:
                        print("Error al decodificar el evento:", event)
                        continue

        # Capturamos específicamente los cortes de conexión y errores HTTP
        except (requests.exceptions.RequestException, requests.exceptions.ChunkedEncodingError) as e:
            print(f"\nConexión interrumpida: {e}")
            print("Reconectando en 5 segundos...\n")
            time.sleep(5)
            continue
        except KeyboardInterrupt:
            print("\nDeteniendo el productor...")
            break
        except Exception as e:
            print(f"\nOcurrió un error inesperado: {e}")
            break       

    print("Vaciando la cola de mensajes hacia Kafka...")
    producer.flush() # Bloquea la ejecución hasta que todos los mensajes en la cola hayan sido enviados y sus callbacks procesados.
    print("Productor cerrado.")

if __name__ == "__main__":
    run_producer()