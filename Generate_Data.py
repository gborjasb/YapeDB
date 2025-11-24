"""
GENERADOR MASIVO DE DATOS YAPE
"""

from datetime import timedelta, datetime
import hashlib
import psycopg2
from psycopg2.extras import execute_values, execute_batch
from faker import Faker
import random
import numpy as np
from tqdm import tqdm
import time

# ============================================================
# CONFIGURACIÓN
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "database": "yape_database",
    "port": 5434,
    "user": "gmborjasb",
    "password": "MomoYMoka"
}

NUM_ACTORES = 1_000_000
# NUM_ACTORES = 100_000
# NUM_ACTORES = 10_000
# NUM_ACTORES = 1_000
PORC_PERSONAS = 0.7
BATCH_SIZE = 5000

NUM_PERSONAS = int(NUM_ACTORES * PORC_PERSONAS)
NUM_EMPRESAS = NUM_ACTORES - NUM_PERSONAS

faker = Faker("es_MX")
Faker.seed(42)
random.seed(42)
np.random.seed(42)


def conectar():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SET search_path TO yape_1_000_000;")
    cur.execute("SET synchronous_commit TO OFF;")
    cur.execute("SET work_mem = '256MB';")
    conn.commit()
    return conn, cur


def cerrar(conn, cur):
    cur.close()
    conn.close()


print("=" * 70)
print(f"🚀 GENERACIÓN COMPLETA - 100K ACTORES")
print(f"   Total: {NUM_ACTORES:,} | Personas: {NUM_PERSONAS:,} | Empresas: {NUM_EMPRESAS:,}")
print("=" * 70)

inicio_total = time.time()

# ============================================================
# PARTE 1 – ACTORES
# ============================================================

print("\n📌 PARTE 1: Actores")
inicio = time.time()

conn, cur = conectar()

cur.execute("""
    INSERT INTO actor_yape (id_actor)
    SELECT generate_series(
        COALESCE((SELECT MAX(id_actor) FROM actor_yape), 0) + 1,
        COALESCE((SELECT MAX(id_actor) FROM actor_yape), 0) + %s
    );
""", (NUM_ACTORES,))
conn.commit()

cur.execute("SELECT id_actor FROM actor_yape ORDER BY id_actor DESC LIMIT %s;", (NUM_ACTORES,))
actor_ids = [r[0] for r in cur.fetchall()]
actor_ids.reverse()

person_ids = actor_ids[:NUM_PERSONAS]
company_ids = actor_ids[NUM_PERSONAS:]

print("▶ Personas...")
used_dni = set()
personas_data = []

for aid in tqdm(person_ids, desc="Personas", ncols=70):
    while True:
        dni = str(random.randint(70000000, 79999999))
        if dni not in used_dni:
            used_dni.add(dni)
            break
    personas_data.append((aid, dni, f"user{aid}@yape.com"))

    if len(personas_data) >= BATCH_SIZE:
        execute_values(cur, "INSERT INTO persona (id_actor, dni, correo) VALUES %s", personas_data, page_size=1000)
        conn.commit()
        personas_data.clear()

if personas_data:
    execute_values(cur, "INSERT INTO persona (id_actor, dni, correo) VALUES %s", personas_data, page_size=1000)
    conn.commit()

print("▶ Empresas...")

used_ruc = set()
empresas_data = []

for aid in tqdm(company_ids, desc="Empresas", ncols=70):
    while True:
        ruc = str(random.randint(10000000000, 19999999999))
        if ruc not in used_ruc:
            used_ruc.add(ruc)
            break
    empresas_data.append((aid, ruc, f"Empresa {aid} SAC", "Retail", f"Comercio {aid}"))

    if len(empresas_data) >= BATCH_SIZE:
        execute_values(cur, "INSERT INTO empresa (id_actor, ruc, razon_social, rubro_comercial, nombre_comercial) VALUES %s", empresas_data, page_size=1000)
        conn.commit()
        empresas_data.clear()

if empresas_data:
    execute_values(cur, "INSERT INTO empresa (id_actor, ruc, razon_social, rubro_comercial, nombre_comercial) VALUES %s", empresas_data, page_size=1000)
    conn.commit()


num_small = int(NUM_EMPRESAS * 0.33)
num_ae = int(NUM_EMPRESAS * 0.33)
num_serv = NUM_EMPRESAS - num_small - num_ae

epn_ids = company_ids[:num_small]
ae_ids = company_ids[num_small:num_small+num_ae]
serv_ids = company_ids[num_small+num_ae:]

execute_values(cur, "INSERT INTO empresa_pequeno_negocio (id_actor, dni_duenho, correo_contacto, celular_contacto) VALUES %s",
               [(aid, str(random.randint(70000000, 79999999)), f"c{aid}@negocio.com", str(random.randint(900000000, 999999999))) for aid in epn_ids], page_size=1000)
execute_values(cur, "INSERT INTO empresa_acceso_empresarial (id_actor) VALUES %s", [(aid,) for aid in ae_ids], page_size=1000)
execute_values(cur, "INSERT INTO empresa_servicios (id_actor) VALUES %s", [(aid,) for aid in serv_ids], page_size=1000)
conn.commit()

cerrar(conn, cur)
print(f"  ✔ Parte 1: {time.time() - inicio:.1f}s")

# ============================================================
# PARTE 2 – FUNCIONALIDADES
# ============================================================

print("\n📌 PARTE 2: Funcionalidades")
inicio = time.time()

conn, cur = conectar()

FUNCIONALIDADES = [
    ("Enviar dinero", "Permite transferencias entre billeteras."),
    ("Recibir dinero", "Registro de pagos recibidos."),
    ("Consultar saldo", "Muestra el saldo disponible."),
    ("Consultar movimientos", "Historial de operaciones."),
    ("Pago de servicios", "Pago a empresas de servicios."),
    ("Pago con QR", "Permite pagar usando QR."),
    ("Generar QR estático", "Crea un QR permanente."),
    ("Generar QR dinámico", "Crea QR con monto y validez."),
    ("Reportes empresariales", "Reportes de negocio."),
    ("Notificación de envío de dinero", "Aviso por envíos."),
    ("Notificación de recepción de dinero", "Aviso por recepciones."),
]

cur.execute("SELECT nombre, id_funcionalidad FROM funcionalidad;")
existing_funcs = {row[0]: row[1] for row in cur.fetchall()}

func_ids = existing_funcs.copy()
new_funcs = [f for f in FUNCIONALIDADES if f[0] not in existing_funcs]

if new_funcs:
    execute_values(cur, "INSERT INTO funcionalidad (nombre, descripcion) VALUES %s RETURNING nombre, id_funcionalidad;", new_funcs)
    for nombre, fid in cur.fetchall():
        func_ids[nombre] = fid
    conn.commit()

cur.execute("SELECT id_actor FROM persona;")
PERSONAS = [r[0] for r in cur.fetchall()]

cur.execute("SELECT id_actor FROM empresa_pequeno_negocio;")
EPN = [r[0] for r in cur.fetchall()]

cur.execute("SELECT id_actor FROM empresa_acceso_empresarial;")
EAE = [r[0] for r in cur.fetchall()]

cur.execute("SELECT id_actor FROM empresa_servicios;")
ES = [r[0] for r in cur.fetchall()]

FUNC_P = ["Enviar dinero", "Recibir dinero", "Consultar saldo", "Pago de servicios", "Pago con QR"]
FUNC_EPN = ["Recibir dinero", "Consultar saldo", "Pago con QR", "Reportes empresariales"]
FUNC_EAE = ["Recibir dinero", "Generar QR dinámico", "Reportes empresariales"]
FUNC_ES = ["Recibir dinero", "Consultar saldo", "Reportes empresariales"]

def asignar_batch(actor_list, func_names, desc):
    data = []
    for actor_id in actor_list:
        for nombre in func_names:
            data.append((actor_id, func_ids[nombre]))

    for i in range(0, len(data), 10000):
        chunk = data[i:i+10000]
        execute_values(cur, "INSERT INTO a_funcionalidad (id_actor, id_funcionalidad) VALUES %s ON CONFLICT DO NOTHING", chunk, page_size=1000)
        conn.commit()

asignar_batch(PERSONAS, FUNC_P, "Func.Personas")
asignar_batch(EPN, FUNC_EPN, "Func.EPN")
asignar_batch(EAE, FUNC_EAE, "Func.EAE")
asignar_batch(ES, FUNC_ES, "Func.Servicios")

cerrar(conn, cur)
print(f"  ✔ Parte 2: {time.time() - inicio:.1f}s")

# ============================================================
# PARTE 3 – BILLETERAS
# ============================================================

print("\n📌 PARTE 3: Billeteras")
inicio = time.time()

conn, cur = conectar()

cur.execute("SELECT id_actor FROM persona;")
ACTORES_PERSONA = [r[0] for r in cur.fetchall()]

cur.execute("SELECT id_actor FROM empresa_pequeno_negocio;")
ACTORES_EPN = [r[0] for r in cur.fetchall()]

cur.execute("SELECT id_actor FROM empresa_acceso_empresarial;")
ACTORES_EAE = [r[0] for r in cur.fetchall()]

cur.execute("SELECT id_actor FROM empresa_servicios;")
ACTORES_ES = [r[0] for r in cur.fetchall()]

billeteras_yape_data = []
billeteras_persona_data = []

for id_actor in tqdm(ACTORES_PERSONA, desc="Bill.Persona", ncols=70):
    celular = str(random.randint(900000000, 989999999))
    saldo = round(random.uniform(100, 5000), 2)
    billeteras_yape_data.append((id_actor, celular, "Yape", "Activo", True, saldo))
    billeteras_persona_data.append((id_actor, "DNI", 500, 5000, 100, False, "Basico"))

execute_values(cur, "INSERT INTO billetera_yape (id_actor, celular, origen_billetera, estado, permite_interoperabilidad, saldo) VALUES %s", billeteras_yape_data, page_size=5000)

cur.execute("SELECT id_actor, id_billetera FROM billetera_yape WHERE id_actor = ANY(%s);", (ACTORES_PERSONA,))
actor_to_billetera = {row[0]: row[1] for row in cur.fetchall()}

billeteras_persona_final = [(actor_to_billetera[aid], *rest) for aid, *rest in billeteras_persona_data]
execute_values(cur, "INSERT INTO billetera_persona (id_billetera, metodo_registro, limite_diario, limite_mensual_recaudacion, limite_por_operacion, bloqueado_fraude, nivel_verificacion) VALUES %s", billeteras_persona_final, page_size=5000)
conn.commit()

def crear_billeteras_empresa(actores_list):
    billeteras_data = [(id_actor, str(random.randint(900000000, 989999999)), "Yape", "Activo", False, round(random.uniform(1000, 50000), 2)) for id_actor in actores_list]
    execute_values(cur, "INSERT INTO billetera_yape (id_actor, celular, origen_billetera, estado, permite_interoperabilidad, saldo) VALUES %s", billeteras_data, page_size=5000)

    cur.execute("SELECT id_actor, id_billetera FROM billetera_yape WHERE id_actor = ANY(%s);", (actores_list,))
    actor_to_bill = {row[0]: row[1] for row in cur.fetchall()}

    empresa_data = [(actor_to_bill[aid], str(random.randint(10000000, 99999999)), 0.03, 100000) for aid in actores_list]
    execute_values(cur, "INSERT INTO billetera_empresa (id_billetera, cuenta_recaudacion, tasa_comision, limite_mensual_recaudacion) VALUES %s", empresa_data, page_size=5000)
    conn.commit()

crear_billeteras_empresa(ACTORES_EPN)
crear_billeteras_empresa(ACTORES_EAE)
crear_billeteras_empresa(ACTORES_ES)

total_other = int(len(ACTORES_PERSONA) * 0.30)
other_data_yape = [(None, str(random.randint(900000000, 989999999)), random.choice(["Plin", "Tunki"]), "Activo", True, round(random.uniform(50, 2000), 2)) for _ in range(total_other)]

execute_values(cur, "INSERT INTO billetera_yape (id_actor, celular, origen_billetera, estado, permite_interoperabilidad, saldo) VALUES %s RETURNING id_billetera", other_data_yape, page_size=5000)

other_ids = [row[0] for row in cur.fetchall()]
other_data_ext = [(bid, f"EXT{bid}", f"Usuario{bid}") for bid in other_ids]

execute_values(cur, "INSERT INTO billetera_other_persona (id_billetera, id_externo, nombre_externo) VALUES %s", other_data_ext, page_size=5000)
conn.commit()

cur.execute("SELECT id_billetera FROM billetera_yape;")
TODAS_BILLETERAS = [r[0] for r in cur.fetchall()]

def hash_password(x):
    return hashlib.sha256(x.encode()).hexdigest()

credenciales_data = []

for bid in tqdm(TODAS_BILLETERAS, desc="Credenciales", ncols=70):
    raw = f"pass{bid}"
    hashed = hash_password(raw)
    fecha_cre = datetime.now() - timedelta(days=random.randint(1, 365))

    credenciales_data.append((bid, 1, "PIN", hashed, fecha_cre, None, "Activa"))

    if len(credenciales_data) >= BATCH_SIZE:
        execute_values(cur, "INSERT INTO credencial (id_billetera, id_credencial, tipo, hash_valor, fecha_creacion, fecha_expiracion, estado) VALUES %s", credenciales_data, page_size=1000)
        conn.commit()
        credenciales_data.clear()

if credenciales_data:
    execute_values(cur, "INSERT INTO credencial (id_billetera, id_credencial, tipo, hash_valor, fecha_creacion, fecha_expiracion, estado) VALUES %s", credenciales_data, page_size=1000)
    conn.commit()

cur.execute("SELECT by.id_billetera FROM billetera_yape by JOIN empresa_acceso_empresarial ae ON by.id_actor = ae.id_actor;")
BILLETERAS_AE = [r[0] for r in cur.fetchall()]

qr_data = [(f"QRD{bid}{random.randint(1000, 9999)}", True) for bid in BILLETERAS_AE]
execute_values(cur, "INSERT INTO qr (codigo_qr, activo) VALUES %s RETURNING id_qr;", qr_data)

qr_ids = [row[0] for row in cur.fetchall()]
qr_dinamico_data = [(qr_id, round(random.uniform(10, 500), 2), 600) for qr_id in qr_ids]

execute_values(cur, "INSERT INTO qr_dinamico (id_qr, monto_fijo, tiempo_validez) VALUES %s", qr_dinamico_data, page_size=1000)
conn.commit()

cerrar(conn, cur)
print(f"  ✔ Parte 3: {time.time() - inicio:.1f}s")

# ============================================================
# PARTE 4 – TRANSACCIONES
# ============================================================

print("\n📌 PARTE 4: Transacciones")
inicio = time.time()

conn, cur = conectar()

cur.execute("SELECT id_billetera FROM billetera_persona;")
billetera_persona = [row[0] for row in cur.fetchall()]

cur.execute("SELECT id_billetera FROM billetera_other_persona;")
billetera_other = [row[0] for row in cur.fetchall()]

cur.execute("SELECT by.id_billetera FROM billetera_yape by JOIN empresa_pequeno_negocio epn ON by.id_actor = epn.id_actor;")
billetera_epn = [row[0] for row in cur.fetchall()]

cur.execute("UPDATE billetera_yape SET saldo = ROUND((RANDOM() * 5000 + 1000)::numeric, 2) WHERE estado = 'Activo';")
conn.commit()

TARGET_TOTAL = 10_000_000
# TARGET_TOTAL = 1_000_000
# TARGET_TOTAL = 100_000
# TARGET_TOTAL = 10_000
total_p2p = int(TARGET_TOTAL * 0.70)

emisores_p2p = billetera_persona + billetera_other + billetera_epn
receptores_p2p = emisores_p2p.copy()

cur.execute("ALTER TABLE transaccion_persona DISABLE TRIGGER ALL;")
cur.execute("ALTER TABLE operacion_yape DISABLE TRIGGER ALL;")
conn.commit()

np_random = np.random.default_rng(42)

emisor_indices = np_random.integers(0, len(emisores_p2p), size=total_p2p)
receptor_indices = np_random.integers(0, len(receptores_p2p), size=total_p2p)
montos = np.clip(np_random.normal(80, 30, size=total_p2p), 1, 500).round(2)
tipos = np_random.choice(["Yape", "Plin", "Tunki"], size=total_p2p)
codigos = np_random.integers(1000, 9999, size=total_p2p)

valid_mask = emisor_indices != receptor_indices
emisor_indices = emisor_indices[valid_mask]
receptor_indices = receptor_indices[valid_mask]
montos = montos[valid_mask]
tipos = tipos[valid_mask]
codigos = codigos[valid_mask]

total_p2p_final = len(emisor_indices)
fecha_tx = datetime.now()

CHUNK_SIZE = 10000

for i in tqdm(range(0, total_p2p_final, CHUNK_SIZE), desc="  P2P", ncols=80):
    chunk_end = min(i + CHUNK_SIZE, total_p2p_final)

    transacciones_chunk = [(fecha_tx, "Exitosa", float(montos[j]), f"OP{j:010d}") for j in range(i, chunk_end)]

    execute_values(cur, "INSERT INTO transaccion_yape (fecha_hora, estado, monto, numero_operacion) VALUES %s RETURNING id_transaccion", transacciones_chunk, page_size=5000)

    tx_ids = [row[0] for row in cur.fetchall()]

    tx_persona_chunk = [(tx_ids[k], str(codigos[i + k]), tipos[i + k], None, None, None) for k in range(len(tx_ids))]

    execute_values(cur, "INSERT INTO transaccion_persona (id_transaccion, codigo_seguridad, tipo_destino, numero_receptor, nombre_receptor, nombre_emisor) VALUES %s", tx_persona_chunk, page_size=5000)

    operaciones_chunk = [(tx_ids[k], emisores_p2p[emisor_indices[i + k]], receptores_p2p[receptor_indices[i + k]]) for k in range(len(tx_ids))]

    execute_values(cur, "INSERT INTO operacion_yape (id_transaccion, id_billetera_emisor, id_billetera_receptor) VALUES %s", operaciones_chunk, page_size=5000)

    conn.commit()

cur.execute("ALTER TABLE transaccion_persona ENABLE TRIGGER ALL;")
cur.execute("ALTER TABLE operacion_yape ENABLE TRIGGER ALL;")
conn.commit()

cerrar(conn, cur)
print(f"  ✔ Parte 4: {time.time() - inicio:.1f}s ({total_p2p_final:,} transacciones)")


# ============================================================
# PARTE 5 – NOTIFICACIONES
# ============================================================

print("\n📌 PARTE 5: Notificaciones")
inicio = time.time()

conn, cur = conectar()

cur.execute("""
    SELECT ty.id_transaccion, ty.monto, oy.id_billetera_emisor, oy.id_billetera_receptor
    FROM transaccion_yape ty
    JOIN operacion_yape oy ON ty.id_transaccion = oy.id_transaccion
    WHERE ty.estado = 'Exitosa'
    AND RANDOM() < 0.3
    LIMIT 1000000;
""")

transacciones = cur.fetchall()
print(
    f"  → Generando notificaciones para {len(transacciones):,} transacciones")

notif_ids = []
for id_tx, monto, _, _ in tqdm(transacciones, desc="Notificaciones", ncols=70):
    # Envío
    cur.execute(
        "INSERT INTO notificacion (mensaje, estado, canal) VALUES (%s, %s, %s) RETURNING id_notificacion;",
        (f"Enviaste S/ {monto:.2f}", "Enviada", "App")
    )
    id_envio = cur.fetchone()[0]
    # Recepción
    cur.execute(
        "INSERT INTO notificacion (mensaje, estado, canal) VALUES (%s, %s, %s) RETURNING id_notificacion;",
        (f"Recibiste S/ {monto:.2f}", "Enviada", "App")
    )
    id_recepcion = cur.fetchone()[0]
    notif_ids.append((id_envio, id_recepcion, id_tx, monto))
    if len(notif_ids) % 1000 == 0:
        conn.commit()
conn.commit()

notif_tx_data = []
for id_envio, id_recepcion, id_tx, monto in notif_ids:
    notif_tx_data.append((id_envio, id_tx, monto, "Exitosa", "Envio"))
    notif_tx_data.append((id_recepcion, id_tx, monto, "Exitosa", "Recepcion"))

execute_values(
    cur,
    "INSERT INTO notificacion_transaccion (id_notificacion, id_transaccion, monto, resultado, tipo_operacion) VALUES %s",
    notif_tx_data,
    page_size=5000
)
conn.commit()

cerrar(conn, cur)
print(f"  ✔ Parte 5: {len(notif_tx_data):,} notificaciones")
tiempo_total = time.time() - inicio_total

print("\n" + "=" * 70)
print("🎉 COMPLETADO - 100K ACTORES")
print("=" * 70)
print(f"⏱️  Tiempo total: {tiempo_total/60:.1f} minutos")
print("=" * 70)