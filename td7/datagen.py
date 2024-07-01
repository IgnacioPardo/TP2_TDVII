""" Data generator for MercadoPago
"""

# pylint: disable=W0718
# pylint: disable=W0719
# pylint: disable=C0301
# pylint: disable=E0401

# Librerias

# import os
import random
from datetime import datetime, timedelta
from typing import List, Tuple
from tqdm import tqdm

import faker  # type: ignore

from sqlalchemy import create_engine  # type: ignore
from sqlalchemy import text   # type: ignore
from sqlalchemy.orm import sessionmaker   # type: ignore

from td7.config import POSTGRES_CONN_STRING

random.seed(42)

fake = faker.Faker()

# Coneccion a la base de datos
print(fake)

pg = create_engine(POSTGRES_CONN_STRING)

Session = sessionmaker(bind=pg)

# Mercadopago.SQL

# import os
# raise Exception(os.getcwd())

# Crear tablas
with open("sql/create_tables.sql", encoding="utf-8") as f:
    sql = f.read()
    pg.execute(text(sql))

# Crear datos de prueba


# Crear identificadores - DNI, CUIT, CVU/CBU, ALIAS


def random_dni():
    """ Genera un DNI aleatorio

    Returns:
        str: DNI
    """
    return str(random.randint(10**7, 10**8 - 1))


def random_cuit():
    """ Genera un CUIT aleatorio

    Returns:
        str: CUIT
    """

    return (
        str(random.randint(10, 99))
        + "-"
        + random_dni()
        + "-"
        + str(random.randint(0, 9))
    )


def random_cu():
    """ Genera un CVU/CBU aleatorio

    Returns:
        str: CVU/CBU
    """

    return (
        "0"
        + str(random.randint(0, 999))
        + "0"
        + str(random.randint(0, 9999))
        + str(random.randint(0, 9))
        + str(random.randint(10**12, 10**13 - 1))
    )


def random_alias():
    """ Genera un alias aleatorio

    Returns:
        str: Alias
    """

    return ".".join(fake.words(nb=3))


# Crear usuarios de prueba


def insert_clave_uniforme(cu: str, alias: str, es_virtual: bool) -> None:
    """Inserta una clave uniforme en la base de datos

    Args:
        cu (str): Clave Uniforme
        alias (str): Alias
        esVirtual (bool): Es virtual o no
    """

    pg.execute("INSERT INTO Clave VALUES (%s, %s, %s)", (cu, alias, es_virtual))


def create_identifiers() -> Tuple[str, str, str]:
    """Crea identificadores aleatorios

    Returns:
        Tuple[str, str, str]: CU, Alias, CUIT
    """

    cu = random_cu()
    alias = random_alias()
    cuit = random_cuit()

    return cu, alias, cuit


def create_usuario() -> Tuple[str, str, str, str, str, str, str, str]:
    """Crea un usuario aleatorio

    Returns:
        Tuple[str, str, str, str, str, str, str, str]:
        CVU,
        Alias,
        CUIT,
        Email,
        Nombre,
        Apellido,
        Username,
        Password
    """

    cvu, alias, cuit = create_identifiers()
    insert_clave_uniforme(cvu, alias, True)
    pg.execute(
        "INSERT INTO Usuario VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            cvu,
            cuit,
            (email := fake.email()),
            (first_name := fake.first_name()),
            (last_name := fake.last_name()),
            (user_name := fake.user_name()),
            (password := fake.password()),
            0,
            fake.date_this_century(),
        ),
    )
    return cvu, alias, cuit, email, first_name, last_name, user_name, password


def create_cuenta_bancaria() -> Tuple[str, str]:
    """Crea una cuenta bancaria aleatoria

    Returns:
        Tuple[str, str]: CU, Alias
    """
    cvu, alias, _ = create_identifiers()
    insert_clave_uniforme(cvu, alias, False)
    pg.execute("INSERT INTO CuentaBancaria VALUES (%s, %s)", (cvu, fake.company()))
    return cvu, alias


def create_cuenta_bancaria_mercadopago() -> Tuple[str, str]:
    """Crea una cuenta bancaria de MercadoPago

    Returns:
        Tuple[str, str]: CU, Alias
    """
    cvu = len(random_cu()) * "0"
    alias = "MercadoPago"
    insert_clave_uniforme(cvu, alias, False)
    pg.execute("INSERT INTO CuentaBancaria VALUES (%s, %s)", (cvu, "MercadoPago"))
    return cvu, alias


def get_cuenta_bancaria_mercadopago():
    """Obtiene la cuenta bancaria de MercadoPago

    Returns:
        str: CVU
    """
    return pg.execute(
        """SELECT CuentaBancaria.clave_uniforme
        FROM CuentaBancaria INNER JOIN Clave ON CuentaBancaria.clave_uniforme = Clave.clave_uniforme
        WHERE Clave.alias = 'MercadoPago'
        """
    ).fetchone()[0]


def create_proveedor_servicio() -> Tuple[str, str]:
    """Crea un proveedor de servicio aleatorio

    Returns:
        Tuple[str, str]: CU, Alias
    """
    cvu, alias, _ = create_identifiers()
    insert_clave_uniforme(cvu, alias, False)
    pg.execute(
        "INSERT INTO ProveedorServicio VALUES (%s, %s, %s, %s)",
        (cvu, fake.company(), fake.word(), fake.date_this_century()),
    )
    return cvu, alias


def add_tarjeta(cvu: str, number: str, cvv: str, vencimiento: str):
    """Agrega una tarjeta a la base de datos

    Args:
        cvu (str): CVU del usuario al que pertenece la tarjeta
        number (str): Número de tarjeta
        cvv (str): CVV
        vencimiento (str): Fecha de vencimiento
    """

    pg.execute(
        "INSERT INTO Tarjeta VALUES (%s, %s, %s, %s)", (number, vencimiento, cvv, cvu)
    )


def create_tarjeta(cvu: str) -> Tuple[str, str, str]:
    """Crea una tarjeta aleatoria

    Args:
        cvu (str): CVU del usuario al que pertenece la tarjeta

    Returns:
        Tuple[str, str, str]: Número, CVV, Vencimiento
    """

    add_tarjeta(
        cvu,
        (num := fake.credit_card_number()),
        (cvv := fake.credit_card_security_code()),
        (vencimiento := str(fake.date_between(start_date="-1y", end_date="+1y"))),
    )
    return num, cvv, vencimiento


# Función para hacer la transacción debitada de un usuario


def create_transaction_between_users(
    amount: float,
    description: str,
    sender_cu: str = "",
    reciever_cu: str = "",
    sender_alias: str = "",
    reciever_alias: str = "",
    card_number: str = "",
    fecha: datetime = datetime.now(),
) -> int:
    """Debita una cantidad de dinero de un usuario y la transfiere a otro

    Args:
        sender (str): CVU del usuario que envía el dinero
        reciever (str): CVU del usuario que recibe el dinero
        amount (float): Cantidad de dinero
        description (str): Descripción de la transacción

    Returns:
        int: Código de la transacción
    """

    if not (sender_cu or sender_alias):
        raise Exception("Invalid Sender - No CU or Alias")

    if not (reciever_cu or reciever_alias):
        raise Exception("Invalid Reciever - No CU or Alias")

    if sender_cu != "":
        # Check if sender exists
        if not pg.execute(
            "SELECT clave_uniforme FROM Usuario WHERE clave_uniforme = %s", (sender_cu,)
        ).fetchone():
            raise Exception("Invalid Sender - Not Found")
        # Check if the sender is virtual
        if not pg.execute(
            "SELECT esVirtual FROM Clave WHERE clave_uniforme = %s", (sender_cu,)
        ).fetchone()[0]:
            raise Exception("Invalid Sender - Not Virtual")

    if sender_alias != "" and sender_cu == "":
        sender_cu = pg.execute(
            "SELECT clave_uniforme FROM Clave WHERE alias = %s", (sender_alias,)
        ).fetchone()[0]
        if not sender_cu:
            raise Exception("Invalid Sender - Alias not found")

    if reciever_alias != "" and reciever_cu == "":
        reciever_cu = pg.execute(
            "SELECT clave_uniforme FROM Clave WHERE alias = %s", (reciever_alias,)
        ).fetchone()[0]
        if not reciever_cu:
            raise Exception("Invalid Reciever - Alias not found")

    pg.execute(
        """INSERT INTO Transaccion (CU_Origen, CU_Destino, monto, fecha, descripcion, estado, es_con_tarjeta, numero, interes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            sender_cu,
            reciever_cu,
            amount,
            fecha,
            description,
            (
                "Rechazada"
                if pg.execute(
                    "SELECT saldo FROM Usuario WHERE clave_uniforme = %s", (sender_cu,)
                ).fetchone()[0]
                < amount
                else "Pendiente"
            ),
            card_number != "",
            None if card_number == "" else card_number,
            None if card_number == "" else random.randint(5, 10),
        ),
    )

    # El siguiente código se ejecuta directamente como un trigger en la base de datos

    # pg.execute(
    #     "UPDATE Usuario SET saldo = saldo - %s WHERE clave_uniforme = %s",
    #     (amount, sender),
    # )

    # pg.execute(
    #     "UPDATE Usuario SET saldo = saldo + %s WHERE clave_uniforme = %s",
    #     (amount, reciever),
    # )

    return pg.execute(
        "SELECT codigo FROM Transaccion ORDER BY codigo DESC LIMIT 1"
    ).fetchone()[0]


# Depositar dinero en una cuenta desde una cuenta de bancaria


def create_transaccion_deposit(
    user: str,
    cbu_cuenta_bancaria: str,
    amount: float,
    description: str,
    fecha: datetime = datetime.now(),
) -> Tuple[str, float]:
    """Deposita dinero en la cuenta de un usuario

    Args:
        user (str): CVU del usuario
        amount (float): Cantidad de dinero
        description (str): Descripción de la transacción

    Returns:
        Tuple[str, float]: Código de la transacción, Saldo final
    """

    pg.execute(
        """INSERT INTO Transaccion (CU_Origen, CU_Destino, monto, fecha, descripcion, estado, es_con_tarjeta, numero, interes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            cbu_cuenta_bancaria,
            user,
            amount,
            fecha,
            description,
            "Completada",
            False,
            None,
            None,
        ),
    )

    # El siguiente código se ejecuta directamente como un trigger en la base de datos

    # pg.execute(
    #     "UPDATE Usuario SET saldo = saldo + %s WHERE clave_uniforme = %s",
    #     (amount, user),
    # )

    return (
        str(
            pg.execute(
                "SELECT codigo FROM Transaccion ORDER BY codigo DESC LIMIT 1"
            ).fetchone()[0]
        ),
        float(
            pg.execute(
                "SELECT saldo FROM Usuario WHERE clave_uniforme = %s", (user,)
            ).fetchone()[0]
        ),
    )


# Pagar un servicio


def create_transaccion_pay_service(
    user: str,
    service_name: str,
    amount: float,
    description: str,
    fecha: datetime = datetime.now(),
) -> Tuple[str, float]:
    """Paga un servicio

    Args:
        user (str): CVU del usuario
        service_name (str): Nombre del servicio
        amount (float): Cantidad de dinero
        description (str): Descripción de la transacción

    Returns:
        Tuple[str, float]: Código de la transacción, Saldo final
    """

    # Check for balance first
    if (
        pg.execute(
            "SELECT saldo FROM Usuario WHERE clave_uniforme = %s", (user,)
        ).fetchone()[0]
        < amount
    ):
        raise Exception("Not enough balance")

    # Hallar el cbu del servicio por su nombre
    cbu_servicio = pg.execute(
        "SELECT clave_uniforme FROM ProveedorServicio WHERE nombre_empresa = %s",
        (service_name,),
    ).fetchone()[0]

    pg.execute(
        """INSERT INTO Transaccion (CU_Origen, CU_Destino, monto, fecha, descripcion, estado, es_con_tarjeta, numero, interes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            user,
            cbu_servicio,
            amount,
            fecha,
            description,
            "Pendiente",
            False,
            None,
            None,
        ),
    )

    pg.execute(
        "UPDATE Usuario SET saldo = saldo - %s WHERE clave_uniforme = %s",
        (amount, user),
    )

    return (
        str(
            pg.execute(
                "SELECT codigo FROM Transaccion ORDER BY codigo DESC LIMIT 1"
            ).fetchone()[0]
        ),
        float(
            pg.execute(
                "SELECT saldo FROM Usuario WHERE clave_uniforme = %s", (user,)
            ).fetchone()[0]
        ),
    )


# Invertir plata en la fecha y generar rendimientos a futuro


def comenzar_inversion(
    cvu: str,
    comienzo_plazo: datetime = datetime.now(),
):
    """ Comienza una inversión

    Args:
        cvu (str): Clave uniforme del usuario
        comienzo_plazo (datetime): Fecha de comienzo del plazo

    Returns:
        int: ID del rendimiento

    """

    # Se pone a invertir el monto total del usuario
    # La fecha del comienzo del plazo es ahora,
    # El fin del plazo es en 1 dia
    # La fecha de pago es el próximo dia hábil
    # La tna es entre 60 y 80
    # El monto es el saldo del usuario

    # Solo se puede tener un rendimiento en un comienzo de plazo dado
    # Si ya hay un rendimiento activo y se va a generar otro, se debe finalizar el anterior

    # Hallar el saldo del usuario
    saldo = pg.execute(
        "SELECT saldo FROM Usuario WHERE clave_uniforme = %s", (cvu,)
    ).fetchone()[0]

    # Hallar si hay un rendimiento activo
    rendimiento_activo = pg.execute(
        "SELECT * FROM Rendimiento WHERE comienzo_plazo = CURRENT_DATE"
    ).fetchone()

    if rendimiento_activo:
        # Finalizar el rendimiento activo
        pg.execute(
            "UPDATE Rendimiento SET fin_plazo = CURRENT_DATE WHERE id = %s",
            (rendimiento_activo[0],),
        )

    # Generar un nuevo rendimiento
    tna = random.uniform(60, 80)
    monto = saldo
    fin_plazo = datetime.now() + timedelta(days=1)

    pg.execute(
        """INSERT INTO Rendimiento (comienzo_plazo, fin_plazo, TNA, monto)
        VALUES (%s, %s, %s, %s)""",
        (
            comienzo_plazo,
            fin_plazo,
            tna,
            monto,
        ),
    )

    # Asociar el rendimiento al usuario
    rendimiento_id = pg.execute(
        "SELECT id FROM Rendimiento ORDER BY id DESC LIMIT 1"
    ).fetchone()[0]

    pg.execute(
        "INSERT INTO RendimientoUsuario VALUES (%s, %s)",
        (cvu, rendimiento_id),
    )

    return rendimiento_id


def pagar_rendimientos_activos_usuario(cvu: str) -> List:
    """
    Paga los rendimientos de un usuario

    Args:
        cvu (str): Clave uniforme del usuario

    Returns:
        List: Lista de los montos pagados
    """

    # Hallar los rendimientos no pagados del usuario, es decir sin fecha de pago
    # JOIN con Rendimiento para hallar los datos del rendimiento

    rendimientos = pg.execute(
        """SELECT Rendimiento.id, fecha_pago, comienzo_plazo, fin_plazo, TNA, monto
        FROM RendimientoUsuario
        JOIN Rendimiento ON RendimientoUsuario.id = Rendimiento.id
        WHERE clave_uniforme = %s AND pago IS FALSE""",
        (cvu,),
    ).fetchall()

    for rendimiento in rendimientos:
        # Pagar el rendimiento
        pg.execute(
            """
            UPDATE Rendimiento
            SET fecha_pago = CURRENT_DATE WHERE id = %s
            RETURNING id, fecha_pago, comienzo_plazo, fin_plazo, TNA, monto
            """,
            (rendimiento[0],),
        ).fetchone()

        # Añadir el rendimiento generado al saldo del usuario
        # El rendimiento es el monto * (1 + TNA / 100) * (fin_plazo - comienzo_plazo) / 365

        # rendimiento_generado = rendimiento[5] * (1 + rendimiento[4] / 100) * (rendimiento[3] - rendimiento[2]).days / 365
        rendimiento_generado = rendimiento[5]
        rendimiento_generado *= 1 + rendimiento[4] / 100
        rendimiento_generado *= (rendimiento[3] - rendimiento[2]).days / 365

        # pg.execute(
        #     "UPDATE Usuario SET saldo = saldo + %s WHERE clave_uniforme = %s",
        #     (rendimiento_generado, cvu),
        # )

        # En vez de actualizar el saldo directamente, se crea una transacción
        # con origen en cuenta MercadoPago y destino en la cuenta del usuario
        create_transaccion_deposit(
            cvu,
            get_cuenta_bancaria_mercadopago(),
            rendimiento_generado,
            "Rendimiento",
        )

    if not rendimientos:
        return []

    # Refetch los rendimientos para mostrar by r[0] for r in rendimientos
    updated_rendimientos = pg.execute(
        """SELECT Rendimiento.id, fecha_pago, comienzo_plazo, fin_plazo, TNA, monto
        FROM RendimientoUsuario
        JOIN Rendimiento ON RendimientoUsuario.id = Rendimiento.id
        WHERE clave_uniforme = %s AND Rendimiento.id IN %s""",
        (cvu, tuple([r[0] for r in rendimientos])),
    ).fetchall()

    return updated_rendimientos


# Batch de Datos para correr las querys

# Batch de Datos para correr las querys

# Crear 100 usuarios, 100 cuentas bancarias, 20 servicios, algunos usuarios tienen tarjetas, algunos generan rendimientos
# Simular el paso de 3 meses


def generate_data(
    num_users: int = 100,
    num_cuentas_bancarias: int = 100,
    num_servicios: int = 20,
    num_transacciones_sin_saldo: int = 20,
    timespan: int = 1,
):
    """Genera datos de prueba

    Args:
        num_users (int): Número de usuarios
        num_cuentas_bancarias (int): Número de cuentas bancarias
        num_servicios (int): Número de servicios
        num_transacciones_sin_saldo (int): Número de transacciones sin saldo
        timespan (int): Duración de la simulación en días
    """

    new_data = {}
    new_data["usrs"] = []
    new_data["cbs"] = []
    new_data["servs"] = []

    create_cuenta_bancaria_mercadopago()

    for _ in tqdm(range(num_users), desc="Usuarios"):
        cvu, alias, cuit, email, first_name, last_name, user_name, password = (
            create_usuario()
        )
        new_data["usrs"].append(
            (cvu, alias, cuit, email, first_name, last_name, user_name, password)
        )

        if random.choice([True, False]):
            create_tarjeta(cvu)

    for _ in tqdm(range(num_cuentas_bancarias), desc="Cuentas Bancarias"):
        cvu, alias = create_cuenta_bancaria()
        new_data["cbs"].append((cvu, alias))

    for _ in tqdm(range(num_servicios), desc="Servicios"):
        cvu, alias = create_proveedor_servicio()
        new_data["servs"].append((cvu, alias))

    # Random try 20 transacciones entre usuarios sin saldo (todavia no se han hecho depósitos)
    for _ in tqdm(range(num_transacciones_sin_saldo), desc="Transacciones sin saldo"):
        try:
            create_transaction_between_users(
                random.randint(0, 100),
                "Transferencia",
                sender_cu=random.choice(new_data["usrs"])[0],
                reciever_cu=random.choice(new_data["usrs"])[0],
            )
        except Exception as e:
            if "Not enough balance" in str(e):
                pass

    # Simular el paso del tiempo
    for days in tqdm(range(timespan), desc="Días"):
        today = datetime.now() - timedelta(days=days)

        for usr in new_data["usrs"]:
            if random.choice([True, False]):
                comenzar_inversion(usr[0], today)

        for serv in new_data["servs"]:
            if random.choice([True, False]):
                try:
                    create_transaccion_pay_service(
                        random.choice(new_data["usrs"])[0],
                        serv[1],
                        random.randint(10, 10000),
                        "Pago de servicio",
                        today,
                    )
                except Exception as e:
                    if "Not enough balance" in str(e):
                        pass

        for usr in new_data["usrs"]:
            pagar_rendimientos_activos_usuario(usr[0])

        # Generar transacciones entre usuarios
        for _ in range(random.randint(5, 20)):
            # Para todos los usuarios entrar plata al sistema
            for usr in new_data["usrs"]:
                create_transaccion_deposit(
                    usr[0],
                    random.choice(new_data["cbs"])[0],
                    random.randint(100, 10000),
                    "Depósito",
                    today,
                )

            # Seleccionar o un alias o un cvu
            sender = random.choice(new_data["usrs"])

            usr2 = random.choice(list(set(new_data["usrs"]) - {sender}))

            alias = ""
            cvu = ""
            if random.random() > 0.8:
                cvu = usr2[0]
            else:
                alias = usr2[1]

            try:
                create_transaction_between_users(
                    random.randint(
                        0,
                        int(
                            pg.execute(
                                "SELECT saldo FROM Usuario WHERE clave_uniforme = %s",
                                (cvu,),
                            ).fetchone()[0]
                        ),
                    ),
                    "Transferencia",
                    sender_cu=sender[0],
                    reciever_alias=alias,
                    reciever_cu=cvu,
                    card_number=random.choice(
                        random.choice(cards)
                        if (
                            cards := pg.execute(
                                "SELECT numero FROM Tarjeta where CU = %s", (sender[0],)
                            ).fetchall()
                        )
                        else [""]
                    ),
                    fecha=today,
                )
            except Exception as e:
                if "Not enough balance" in str(e):
                    pass


if __name__ == "__main__":
    generate_data()
