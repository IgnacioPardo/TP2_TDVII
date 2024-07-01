DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO public;

CREATE TABLE IF NOT EXISTS Clave (
    clave_uniforme VARCHAR(50) PRIMARY KEY,
    alias VARCHAR(50) NOT NULL UNIQUE,
    esVirtual BOOLEAN NOT NULL
);


CREATE TABLE IF NOT EXISTS Usuario (
    clave_uniforme VARCHAR(50) PRIMARY KEY,
    CUIT VARCHAR(50),
    email VARCHAR(50),
    nombre VARCHAR(50),
    apellido VARCHAR(50),
    username VARCHAR(50),
    password VARCHAR(50),
    saldo FLOAT DEFAULT 0,
    fecha_alta DATE,
    FOREIGN KEY (clave_uniforme) REFERENCES Clave(clave_uniforme)
);


CREATE TABLE IF NOT EXISTS CuentaBancaria (
    clave_uniforme VARCHAR(50) PRIMARY KEY,
    banco VARCHAR(50),
    FOREIGN KEY (clave_uniforme) REFERENCES Clave(clave_uniforme)
);


CREATE TABLE IF NOT EXISTS ProveedorServicio (
    clave_uniforme VARCHAR(50) PRIMARY KEY,
    nombre_empresa VARCHAR(50),
    categoria_servicio VARCHAR(50),
    fecha_alta DATE,
    FOREIGN KEY (clave_uniforme) REFERENCES Clave(clave_uniforme)
);


CREATE TABLE IF NOT EXISTS Tarjeta (
    numero VARCHAR(50) PRIMARY KEY,
    vencimiento DATE,
    cvv INTEGER,
    CU VARCHAR(50),
    FOREIGN KEY (CU) REFERENCES Clave(clave_uniforme),
    CHECK (CU IS NOT NULL)
);


-- Add trigger on insert to check if cu is virtual, if not, raise exception

CREATE OR REPLACE FUNCTION check_cu_virtual() 
RETURNS TRIGGER AS $$
BEGIN
    IF (SELECT esVirtual FROM Clave WHERE clave_uniforme = NEW.CU) = FALSE THEN
        RAISE EXCEPTION 'CU is not virtual';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_cu_virtual_trigger
BEFORE INSERT ON Tarjeta
FOR EACH ROW
EXECUTE FUNCTION check_cu_virtual();

CREATE TABLE IF NOT EXISTS Rendimiento (
    id SERIAL PRIMARY KEY,
    fecha_pago DATE,
    comienzo_plazo DATE NOT NULL,
    fin_plazo DATE,
    TNA FLOAT,
    monto FLOAT,
    pago BOOLEAN DEFAULT FALSE NOT NULL
);


CREATE TABLE IF NOT EXISTS RendimientoUsuario (
    clave_uniforme VARCHAR(50),
    id INTEGER,
    PRIMARY KEY (clave_uniforme, id),
    FOREIGN KEY (clave_uniforme) REFERENCES Clave(clave_uniforme),
    FOREIGN KEY (id) REFERENCES Rendimiento(id)
);


CREATE TABLE IF NOT EXISTS Transaccion (
    codigo SERIAL PRIMARY KEY,
    CU_Origen VARCHAR(50),
    CU_Destino VARCHAR(50),
    monto FLOAT,
    fecha DATE,
    descripcion VARCHAR(50),
    estado VARCHAR(50),
    es_con_tarjeta BOOLEAN,
    numero VARCHAR(50),
    interes FLOAT,
    FOREIGN KEY (CU_Origen) REFERENCES Clave(clave_uniforme),
    FOREIGN KEY (CU_Destino) REFERENCES Clave(clave_uniforme),
    FOREIGN KEY (numero) REFERENCES Tarjeta(numero)
);

-- Checkear balances para debitar
CREATE OR REPLACE FUNCTION check_balance()
RETURNS TRIGGER AS $$
BEGIN
    IF (SELECT esVirtual FROM Clave WHERE clave_uniforme = NEW.CU_Origen) = FALSE THEN
        IF (SELECT esVirtual FROM Clave WHERE clave_uniforme = NEW.CU_Destino) = TRUE THEN
            UPDATE Transaccion SET estado = 'Completada' WHERE codigo = NEW.codigo;
        ELSE
            UPDATE Transaccion SET estado = 'Rechazada' WHERE codigo = NEW.codigo;
            RAISE EXCEPTION 'No se registran transacciones entre cuentas no virtuales y no virtuales';
        END IF;
    ELSE
        IF (SELECT saldo FROM Usuario WHERE clave_uniforme = NEW.CU_Origen) < NEW.monto THEN
            UPDATE Transaccion SET estado = 'Rechazada' WHERE codigo = NEW.codigo;
            -- RAISE EXCEPTION 'Not enough balance';
        ELSE
            IF NEW.estado != 'Rechazada' THEN
                UPDATE Transaccion SET estado = 'Completada' WHERE codigo = NEW.codigo;
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_balance_trigger
AFTER INSERT ON Transaccion
FOR EACH ROW
WHEN (NEW.es_con_tarjeta = FALSE)
EXECUTE FUNCTION check_balance();

-- Marcar como aceptadas todas las que son con tarjeta

CREATE OR REPLACE FUNCTION check_desde_tarjeta()
RETURNS TRIGGER AS $$
BEGIN
    IF (SELECT esVirtual FROM Clave WHERE clave_uniforme = NEW.CU_Origen) = TRUE THEN
        UPDATE Transaccion SET estado = 'Completada' WHERE codigo = NEW.codigo;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_desde_tarjeta_trigger
BEFORE INSERT ON Transaccion
FOR EACH ROW
WHEN (NEW.es_con_tarjeta = TRUE)
EXECUTE FUNCTION check_desde_tarjeta();

-- Checkear rendimiento activo
CREATE OR REPLACE FUNCTION check_rendimiento()
RETURNS TRIGGER AS $$
BEGIN
    -- Checkear si el usuario tiene un rendimiento activo, es decir fecha de fin_plazo es posterior a la fecha de la transaccion
    -- Joinear RendimientoUsuario con Rendimiento para obtener el id del rendimiento activo
    IF EXISTS (
        SELECT * FROM RendimientoUsuario JOIN Rendimiento ON RendimientoUsuario.id = Rendimiento.id WHERE clave_uniforme = NEW.CU_Origen AND NEW.fecha < Rendimiento.fin_plazo AND Rendimiento.pago = FALSE 
    ) THEN
        -- Finalizar el rendimiento
        UPDATE Rendimiento 
        SET fin_plazo = NEW.fecha
        WHERE id = (SELECT id FROM RendimientoUsuario WHERE clave_uniforme = NEW.CU_Origen AND fin_plazo IS NULL);
        INSERT INTO Rendimiento (fin_plazo, comienzo_plazo, TNA, monto) 
        VALUES (
            NEW.fecha + INTERVAL '1 day',
            NEW.fecha, 
            -- Select last TNA and monto from the rendimiento
            (SELECT TNA FROM Rendimiento WHERE id = (SELECT id FROM RendimientoUsuario WHERE clave_uniforme = NEW.CU_Origen AND fin_plazo IS NOT NULL)),
            (SELECT monto FROM Rendimiento WHERE id = (SELECT id FROM RendimientoUsuario WHERE clave_uniforme = NEW.CU_Origen AND fin_plazo IS NOT NULL)) - NEW.monto
        ); 
        INSERT INTO RendimientoUsuario (clave_uniforme, id) 
        VALUES (
            NEW.CU_Origen, 
            -- Select id from last Rendimiento inserted
            (SELECT id FROM Rendimiento ORDER BY id DESC LIMIT 1)
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_rendimiento_trigger
AFTER INSERT ON Transaccion
FOR EACH ROW
WHEN (NEW.es_con_tarjeta = FALSE)
EXECUTE FUNCTION check_rendimiento();

-- Checkear si la tarjeta es valida
CREATE OR REPLACE FUNCTION check_tarjeta()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (SELECT * FROM Tarjeta WHERE numero = NEW.numero) THEN
        RAISE EXCEPTION 'Tarjeta no existe';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_tarjeta_trigger
BEFORE INSERT ON Transaccion
FOR EACH ROW
WHEN (NEW.es_con_tarjeta = TRUE)
EXECUTE FUNCTION check_tarjeta();

-- Checkear si la tarjeta es del usuario
CREATE OR REPLACE FUNCTION check_tarjeta_usuario()
RETURNS TRIGGER AS $$
BEGIN
    IF (SELECT CU FROM Tarjeta WHERE numero = NEW.numero) != NEW.CU_Origen THEN
        RAISE EXCEPTION 'Tarjeta no pertenece al usuario';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_tarjeta_usuario_trigger
BEFORE INSERT ON Transaccion
FOR EACH ROW
WHEN (NEW.es_con_tarjeta = TRUE)
EXECUTE FUNCTION check_tarjeta_usuario();

-- Actualizar los saldos de origen y/o destino solo si son Claves Uniformes Virtuales
-- Si la transaccion es con tarjeta, solo se actualiza el saldo del destino si es virtual

CREATE OR REPLACE FUNCTION update_saldos()
RETURNS TRIGGER AS $$
BEGIN
    IF (SELECT esVirtual FROM Clave WHERE clave_uniforme = NEW.CU_Destino) = TRUE THEN
        UPDATE Usuario SET saldo = saldo + NEW.monto WHERE clave_uniforme = NEW.CU_Destino;
    END IF;
    IF NEW.es_con_tarjeta = FALSE THEN
        IF (SELECT esVirtual FROM Clave WHERE clave_uniforme = NEW.CU_Origen) = TRUE THEN
            UPDATE Usuario SET saldo = saldo - NEW.monto WHERE clave_uniforme = NEW.CU_Origen;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_saldos_trigger
AFTER INSERT ON Transaccion
FOR EACH ROW
WHEN (NEW.estado = 'Completada')
EXECUTE FUNCTION update_saldos();


CREATE TABLE IF NOT EXISTS TransaccionTarjeta (
    codigo INTEGER,
    numero VARCHAR(50),
    PRIMARY KEY (codigo, numero),
    FOREIGN KEY (numero) REFERENCES Tarjeta(numero),
    FOREIGN KEY (codigo) REFERENCES Transaccion(codigo)
);
