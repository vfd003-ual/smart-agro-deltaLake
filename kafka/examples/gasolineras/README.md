# Gasolineras — Pipeline Kafka + PostgreSQL

Pipeline de datos en tiempo real que descarga los precios de todas las gasolineras de España desde la API del Ministerio de Industria, los publica en un topic de Kafka y los almacena en PostgreSQL (TimescaleDB + PostGIS) para su consulta y análisis.

## Arquitectura

```
API Ministerio ──▶ gas_producer.py ──▶ Kafka (topic: gasolineras) ──▶ gas_consumer.py ──▶ PostgreSQL
                                                                                            │
                                                                                     gas_postgis.py
                                                                                     (consultas geoespaciales)
```

| Componente | Descripción |
|---|---|
| `gas_producer.py` | Consulta la API REST del Ministerio cada hora y publica cada gasolinera como mensaje JSON en Kafka. |
| `gas_consumer.py` | Lee los mensajes del topic, parsea los datos y los inserta en dos tablas PostgreSQL (`gasolineras` y `precios`). |
| `gas_init.sql` | Script de inicialización: crea las tablas, activa PostGIS y TimescaleDB, y convierte `precios` en hypertable. |
| `gas_postgis.py` | Consulta geoespacial: encuentra las 10 gasolineras más baratas en un radio dado usando PostGIS. |
| `gas_count.sql` | Query auxiliar que cuenta cuántas gasolineras ofrecen cada tipo de combustible. |

## Requisitos previos

1. **Kafka** en ejecución (ver `kafka/compose.yaml`):
   ```bash
   cd kafka && docker compose up -d
   ```

2. **PostgreSQL con TimescaleDB y PostGIS** en ejecución (ver `postgres/compose.yaml`):
   ```bash
   cd postgres && docker compose up -d
   ```

3. **Dependencias Python** (desde el directorio `examples`):
   ```bash
   pip install -r requeriments.txt
   ```

## Puesta en marcha

### 1. Crear la base de datos y las tablas

Abrir **pgAdmin** en [http://localhost:5050](http://localhost:5050) (credenciales por defecto: `postgres@ual.es` / `postgres`) y:

1. Conectarse al servidor PostgreSQL (`localhost:5432`, usuario `postgres`, contraseña `postgres`).
2. Crear una base de datos llamada `gasolineras` (clic derecho en *Databases* → *Create* → *Database…*).
3. Abrir el *Query Tool* sobre la nueva base de datos, pegar el contenido de `gas_init.sql` y ejecutarlo.

Esto crea:
- La extensión **PostGIS** (funciones geoespaciales).
- La extensión **TimescaleDB** (series temporales).
- La tabla `gasolineras` con datos estáticos (nombre, dirección, coordenadas…).
- La tabla `precios` como **hypertable** de TimescaleDB, con los precios de cada combustible indexados por fecha.

### 2. Lanzar el productor

```bash
python gas_producer.py
```

El productor:
- Consulta la API del Ministerio (`ServiciosRESTCarburantes`).
- Publica ~12.000 registros en el topic `gasolineras`.
- Espera 1 hora antes de volver a consultar.
- Reintenta automáticamente cada 5 minutos si la API falla.

### 3. Lanzar el consumidor

En otra terminal:

```bash
python gas_consumer.py
```

El consumidor:
- Lee mensajes del topic `gasolineras` (grupo: `gasolineras-postgres`).
- Inserta/actualiza los datos de cada gasolinera en la tabla `gasolineras`.
- Inserta los precios en la tabla `precios` (sin duplicados gracias a `ON CONFLICT`).

### 4. Consultar gasolineras cercanas

Una vez cargados los datos, se puede buscar las gasolineras más baratas cerca de una ubicación:

```bash
python gas_postgis.py
```

Edita las variables al inicio del archivo para personalizar la búsqueda:

```python
MI_LATITUD = 36.8425      # Tu latitud
MI_LONGITUD = -2.4041     # Tu longitud
RADIO_KM = 20             # Radio de búsqueda en km
COMBUSTIBLE = "precio_gasolina_95_e5"  # Tipo de combustible
```

Los tipos de combustible disponibles se pueden consultar con `gas_count.sql`.

## Modelo de datos

### Tabla `gasolineras`

Datos estáticos de cada estación de servicio (actualización con `UPSERT`).

| Columna | Tipo | Descripción |
|---|---|---|
| `ideess` | `VARCHAR(20)` PK | Identificador único de la estación |
| `rotulo` | `VARCHAR(150)` | Nombre / marca comercial |
| `direccion` | `VARCHAR(250)` | Dirección postal |
| `latitud` / `longitud` | `NUMERIC` | Coordenadas WGS84 |
| `provincia` / `municipio` / `localidad` | `VARCHAR` | Ubicación administrativa |
| … | | Otros campos: CP, horario, margen, tipo de venta, etc. |

### Tabla `precios` (hypertable)

Serie temporal con los precios de cada combustible por gasolinera.

| Columna | Tipo | Descripción |
|---|---|---|
| `ideess` | `VARCHAR(20)` FK | Referencia a `gasolineras` |
| `fecha_consulta` | `TIMESTAMP` PK | Momento de la consulta (UTC) |
| `precio_gasoleo_a` | `NUMERIC` | Precio del Gasóleo A (€/L) |
| `precio_gasolina_95_e5` | `NUMERIC` | Precio de la Gasolina 95 (€/L) |
| … | | 23 tipos de combustible en total |

## Puertos por defecto

| Servicio | Puerto |
|---|---|
| Kafka | `9092` |
| Redpanda Console | `8090` |
| PostgreSQL | `5432` |
| pgAdmin | `5050` |
