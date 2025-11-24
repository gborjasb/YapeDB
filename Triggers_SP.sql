/* ===============================================================
      SISTEMA YAPE — BD OPTIMIZADA CON TRIGGERS + SP
================================================================ */

----------------------------------------------------------------------
-- TRIGGER 1: Evitar auto-transacción
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_no_autotransaccion()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.id_billetera_emisor = NEW.id_billetera_receptor THEN
        RAISE EXCEPTION 'No puedes enviarte dinero a ti mismo.';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_no_autotransaccion ON Transaccion_Yape;
DROP TRIGGER IF EXISTS tr_no_autotransaccion ON Operacion_Yape;

CREATE TRIGGER tr_no_autotransaccion
BEFORE INSERT ON Operacion_Yape
FOR EACH ROW
EXECUTE FUNCTION fn_no_autotransaccion();


----------------------------------------------------------------------
-- TRIGGER 2: Validar receptor existente
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_validar_receptor_existente()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM Billetera_Yape
        WHERE id_billetera = NEW.id_billetera_receptor
    ) THEN
        RAISE EXCEPTION 'La billetera receptora no existe.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_validar_receptor_existente ON Transaccion_Yape;
DROP TRIGGER IF EXISTS tr_validar_receptor_existente ON Operacion_Yape;

CREATE TRIGGER tr_validar_receptor_existente
BEFORE INSERT ON Operacion_Yape
FOR EACH ROW
EXECUTE FUNCTION fn_validar_receptor_existente();


----------------------------------------------------------------------
-- TRIGGER 3: Evitar duplicado de número de operación
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_evitar_tx_duplicada()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM Transaccion_Yape WHERE numero_operacion = NEW.numero_operacion
    ) THEN
        RAISE EXCEPTION 'Número de operación duplicado.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_evitar_tx_duplicada ON Transaccion_Yape;

CREATE TRIGGER tr_evitar_tx_duplicada
BEFORE INSERT ON Transaccion_Yape
FOR EACH ROW
EXECUTE FUNCTION fn_evitar_tx_duplicada();


----------------------------------------------------------------------
-- TRIGGER 4: Validar emisor y receptor válido en P2P
-- Ambos deben ser:
--  • Persona
--  • Other Persona
--  • Empresa Pequeño Negocio
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_p2p_billetera_valida(billetera_id INT)
RETURNS BOOLEAN AS $$
DECLARE
    v_es_persona BOOLEAN;
    v_es_other BOOLEAN;
    v_es_pn BOOLEAN;
BEGIN
    -- 1. Persona
    SELECT EXISTS(
        SELECT 1
        FROM Billetera_Persona
        WHERE id_billetera = billetera_id
    ) INTO v_es_persona;

    -- 2. Other Persona
    SELECT EXISTS(
        SELECT 1
        FROM Billetera_Other_Persona
        WHERE id_billetera = billetera_id
    ) INTO v_es_other;

    -- 3. Empresa Pequeño Negocio
    SELECT EXISTS(
        SELECT 1
        FROM Billetera_Yape byp
        JOIN Empresa_Pequeno_Negocio epn ON epn.id_actor = byp.id_actor
        WHERE byp.id_billetera = billetera_id
    ) INTO v_es_pn;

    RETURN (v_es_persona OR v_es_other OR v_es_pn);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fn_p2p_emisor_receptor_validos()
RETURNS TRIGGER AS $$
DECLARE
    v_e INT;
    v_r INT;
BEGIN
    -- Obtener emisor y receptor desde la operación
    SELECT id_billetera_emisor, id_billetera_receptor
    INTO v_e, v_r
    FROM Operacion_Yape
    WHERE id_transaccion = NEW.id_transaccion;

    -- Validar emisor
    IF NOT fn_p2p_billetera_valida(v_e) THEN
        RAISE EXCEPTION 'Emisor no autorizado en P2P.';
    END IF;

    -- Validar receptor
    IF NOT fn_p2p_billetera_valida(v_r) THEN
        RAISE EXCEPTION 'Receptor no autorizado en P2P.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS tr_p2p_emisor_valido ON Transaccion_Persona;
DROP TRIGGER IF EXISTS tr_p2p_emisor_receptor_validos ON Transaccion_Persona;

CREATE TRIGGER tr_p2p_emisor_receptor_validos
BEFORE INSERT ON Transaccion_Persona
FOR EACH ROW
EXECUTE FUNCTION fn_p2p_emisor_receptor_validos();


----------------------------------------------------------------------
-- TRIGGER 5: Validar destino P2P
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_p2p_destino_valido()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.tipo_destino NOT IN ('Yape', 'Plin', 'Agora', 'Tunki', 'Otro') THEN
        RAISE EXCEPTION 'Destino no válido.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_p2p_destino_valido ON Transaccion_Persona;

CREATE TRIGGER tr_p2p_destino_valido
BEFORE INSERT ON Transaccion_Persona
FOR EACH ROW EXECUTE FUNCTION fn_p2p_destino_valido();


----------------------------------------------------------------------
-- TRIGGER 6: Validar código de seguridad (P2P)
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_p2p_codigo_seguridad()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.codigo_seguridad IS NOT NULL
       AND NEW.codigo_seguridad !~ '^[0-9]{4}$' THEN
        RAISE EXCEPTION 'Código de seguridad inválido.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_p2p_codigo_seguridad ON Transaccion_Persona;

CREATE TRIGGER tr_p2p_codigo_seguridad
BEFORE INSERT ON Transaccion_Persona
FOR EACH ROW EXECUTE FUNCTION fn_p2p_codigo_seguridad();


----------------------------------------------------------------------
-- TRIGGER 7: Emisor debe ser Persona (Servicios)
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_serv_emisor_persona()
RETURNS TRIGGER AS $$
DECLARE v_e INT;
BEGIN
    SELECT id_billetera_emisor INTO v_e
    FROM Operacion_Yape WHERE id_transaccion = NEW.id_transaccion;

    IF NOT EXISTS (SELECT 1 FROM Billetera_Persona WHERE id_billetera = v_e) THEN
        RAISE EXCEPTION 'Solo Personas pueden pagar servicios.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_serv_emisor_persona ON Transaccion_Servicios;

CREATE TRIGGER tr_serv_emisor_persona
BEFORE INSERT ON Transaccion_Servicios
FOR EACH ROW EXECUTE FUNCTION fn_serv_emisor_persona();


----------------------------------------------------------------------
-- TRIGGER 8: Receptor debe ser Empresa_Servicios
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_serv_receptor_valido()
RETURNS TRIGGER AS $$
DECLARE v_r INT;
BEGIN
    SELECT id_billetera_receptor INTO v_r
    FROM Operacion_Yape WHERE id_transaccion = NEW.id_transaccion;

    IF NOT EXISTS (
        SELECT 1
        FROM Billetera_Empresa be
        JOIN Billetera_Yape byp ON be.id_billetera = byp.id_billetera
        JOIN Empresa e ON e.id_actor = byp.id_actor
        JOIN Empresa_Servicios eserv ON eserv.id_actor = e.id_actor
        WHERE be.id_billetera = v_r
    ) THEN
        RAISE EXCEPTION 'Receptor no es Empresa_Servicios.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_serv_receptor_valido ON Transaccion_Servicios;

CREATE TRIGGER tr_serv_receptor_valido
BEFORE INSERT ON Transaccion_Servicios
FOR EACH ROW EXECUTE FUNCTION fn_serv_receptor_valido();


----------------------------------------------------------------------
-- TRIGGER 9: Validar código de servicio
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_serv_codigo()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.codigo IS NULL THEN
        RAISE EXCEPTION 'Código de servicio obligatorio.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_serv_codigo ON Transaccion_Servicios;

CREATE TRIGGER tr_serv_codigo
BEFORE INSERT ON Transaccion_Servicios
FOR EACH ROW EXECUTE FUNCTION fn_serv_codigo();


----------------------------------------------------------------------
-- TRIGGER 10: Emisor debe ser Persona (AE)
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_ae_emisor_persona()
RETURNS TRIGGER AS $$
DECLARE v_e INT;
BEGIN
    SELECT id_billetera_emisor INTO v_e
    FROM Operacion_Yape WHERE id_transaccion = NEW.id_transaccion;

    IF NOT EXISTS (SELECT 1 FROM Billetera_Persona WHERE id_billetera = v_e) THEN
        RAISE EXCEPTION 'Solo Personas pueden pagar AE.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_ae_emisor_persona ON Transaccion_Empresa_AE;

CREATE TRIGGER tr_ae_emisor_persona
BEFORE INSERT ON Transaccion_Empresa_AE
FOR EACH ROW EXECUTE FUNCTION fn_ae_emisor_persona();


----------------------------------------------------------------------
-- TRIGGER 11: Receptor debe ser Empresa_AE
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_ae_receptor()
RETURNS TRIGGER AS $$
DECLARE v_r INT;
BEGIN
    SELECT id_billetera_receptor INTO v_r
    FROM Operacion_Yape WHERE id_transaccion = NEW.id_transaccion;

    IF NOT EXISTS (
        SELECT 1
        FROM Billetera_Empresa be
        JOIN Billetera_Yape byp ON be.id_billetera = byp.id_billetera
        JOIN Empresa e ON e.id_actor = byp.id_actor
        JOIN Empresa_Acceso_Empresarial eae ON eae.id_actor = e.id_actor
        WHERE be.id_billetera = v_r
    ) THEN
        RAISE EXCEPTION 'Receptor no válido (Empresa AE).';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_ae_receptor ON Transaccion_Empresa_AE;

CREATE TRIGGER tr_ae_receptor
BEFORE INSERT ON Transaccion_Empresa_AE
FOR EACH ROW EXECUTE FUNCTION fn_ae_receptor();


----------------------------------------------------------------------
-- TRIGGER 12: Validar fecha_confirmacion > fecha transacción
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_ae_fecha_confirmacion()
RETURNS TRIGGER AS $$
DECLARE v_fecha_tx TIMESTAMP;
BEGIN
    SELECT fecha_hora INTO v_fecha_tx
    FROM Transaccion_Yape WHERE id_transaccion = NEW.id_transaccion;

    IF NEW.fecha_confirmacion IS NOT NULL
       AND NEW.fecha_confirmacion <= v_fecha_tx THEN
        RAISE EXCEPTION 'fecha_confirmacion inválida (debe ser posterior a la fecha de la transacción).';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_ae_fecha_confirmacion ON Transaccion_Empresa_AE;

CREATE TRIGGER tr_ae_fecha_confirmacion
BEFORE INSERT OR UPDATE ON Transaccion_Empresa_AE
FOR EACH ROW
EXECUTE FUNCTION fn_ae_fecha_confirmacion();


----------------------------------------------------------------------
-- TRIGGER 13: Crear QR estático automáticamente (UUID v4)
----------------------------------------------------------------------

-- Función común para crear QR estático para una billetera dada
CREATE OR REPLACE FUNCTION fn_crear_qr_estatico_para_billetera(p_id_billetera INT)
RETURNS VOID AS $$
DECLARE
    v_qr INT;
    v_codigo TEXT;
BEGIN
    v_codigo := 'QR_STATIC_' || gen_random_uuid()::text;

    INSERT INTO QR(codigo_qr, activo)
    VALUES(v_codigo, TRUE)
    RETURNING id_qr INTO v_qr;

    INSERT INTO QR_Estatico(id_qr, id_billetera, activo)
    VALUES(v_qr, p_id_billetera, TRUE);
END;
$$ LANGUAGE plpgsql;


-- Trigger para Billetera_Persona
CREATE OR REPLACE FUNCTION fn_qr_estatico_persona()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM fn_crear_qr_estatico_para_billetera(NEW.id_billetera);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_qr_estatico_persona ON Billetera_Persona;

CREATE TRIGGER tr_qr_estatico_persona
AFTER INSERT ON Billetera_Persona
FOR EACH ROW
EXECUTE FUNCTION fn_qr_estatico_persona();


-- Trigger para Billetera_Empresa asociada a Empresa_Pequeno_Negocio
CREATE OR REPLACE FUNCTION fn_qr_estatico_epn()
RETURNS TRIGGER AS $$
DECLARE
    v_es_pn BOOLEAN;
BEGIN
    SELECT EXISTS(
        SELECT 1
        FROM Billetera_Yape byp
        JOIN Empresa_Pequeno_Negocio epn ON epn.id_actor = byp.id_actor
        WHERE byp.id_billetera = NEW.id_billetera
    ) INTO v_es_pn;

    IF v_es_pn THEN
        PERFORM fn_crear_qr_estatico_para_billetera(NEW.id_billetera);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_qr_estatico ON Billetera_Yape;
DROP TRIGGER IF EXISTS tr_qr_estatico_epn ON Billetera_Empresa;

CREATE TRIGGER tr_qr_estatico_epn
AFTER INSERT ON Billetera_Empresa
FOR EACH ROW
EXECUTE FUNCTION fn_qr_estatico_epn();


----------------------------------------------------------------------
-- TRIGGER 14: Validar origen de billetera
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_billetera_origen()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.id_actor IS NOT NULL AND NEW.origen_billetera <> 'Yape' THEN
        RAISE EXCEPTION 'Billeteras internas deben tener origen Yape.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_billetera_origen ON Billetera_Yape;

CREATE TRIGGER tr_billetera_origen
BEFORE INSERT OR UPDATE ON Billetera_Yape
FOR EACH ROW EXECUTE FUNCTION fn_billetera_origen();


----------------------------------------------------------------------
-- TRIGGER 15: Validar celular obligatorio (Persona y Pequeño Negocio)
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_billetera_celular()
RETURNS TRIGGER AS $$
DECLARE
    v_es_persona BOOLEAN;
    v_es_pn BOOLEAN;
BEGIN
    -- ¿Es Persona?
    SELECT EXISTS(
        SELECT 1 FROM Persona p WHERE p.id_actor = NEW.id_actor
    ) INTO v_es_persona;

    -- ¿Es Empresa Pequeño Negocio?
    SELECT EXISTS(
        SELECT 1 FROM Empresa_Pequeno_Negocio epn WHERE epn.id_actor = NEW.id_actor
    ) INTO v_es_pn;

    IF (v_es_persona OR v_es_pn) AND NEW.celular IS NULL THEN
        RAISE EXCEPTION 'Celular obligatorio para Persona y Empresa Pequeño Negocio.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_billetera_celular ON Billetera_Yape;

CREATE TRIGGER tr_billetera_celular
BEFORE INSERT OR UPDATE ON Billetera_Yape
FOR EACH ROW
EXECUTE FUNCTION fn_billetera_celular();



/* ===============================================================
   🔶 STORED PROCEDURES — LÓGICA PESADA (RÁPIDOS Y SEGUROS)
================================================================ */

----------------------------------------------------------------------
-- Helper: Generar número de operación OP + 15 dígitos ÚNICO
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_generar_numero_operacion()
RETURNS TEXT AS $$
DECLARE
    v_num_op TEXT;
BEGIN
    LOOP
        v_num_op := 'OP' || LPAD(FLOOR(RANDOM() * 1e15)::TEXT, 15, '0');
        EXIT WHEN NOT EXISTS (
            SELECT 1 FROM Transaccion_Yape WHERE numero_operacion = v_num_op
        );
    END LOOP;
    RETURN v_num_op;
END;
$$ LANGUAGE plpgsql;


----------------------------------------------------------------------
-- SP 1: TRANSACCIÓN P2P COMPLETA (validación + saldo + registro)
----------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE yape_realizar_transaccion(
    p_emisor INT,
    p_receptor INT,
    p_monto DECIMAL(10,2),
    p_codigo VARCHAR(4),
    p_tipo_destino VARCHAR(20)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_saldo DECIMAL(10,2);
    v_lim_op DECIMAL(10,2);
    v_lim_dia DECIMAL(10,2);
    v_total_hoy DECIMAL(10,2);
    v_tx_id INT;
    v_num_op TEXT;

    v_numero_receptor VARCHAR(9);
    v_nombre_receptor VARCHAR(50);
    v_nombre_emisor   VARCHAR(50);
BEGIN
    -- ===============================
    -- Generar número de operación OP + 15 dígitos (único)
    -- ===============================
    v_num_op := fn_generar_numero_operacion();

    -- ===============================
    -- 1. Validar saldo emisor
    -- ===============================
    SELECT saldo INTO v_saldo
    FROM Billetera_Yape
    WHERE id_billetera = p_emisor
    FOR UPDATE;

    IF v_saldo < p_monto THEN
        RAISE EXCEPTION 'Saldo insuficiente.';
    END IF;

    -- ===============================
    -- 2. Límite por operación (Persona)
    -- ===============================
    SELECT limite_por_operacion
    INTO v_lim_op
    FROM Billetera_Persona
    WHERE id_billetera = p_emisor;

    IF v_lim_op IS NOT NULL AND p_monto > v_lim_op THEN
        RAISE EXCEPTION 'Límite por operación excedido.';
    END IF;

    -- ===============================
    -- 3. Límite diario (Persona)
    -- ===============================
    SELECT limite_diario
    INTO v_lim_dia
    FROM Billetera_Persona
    WHERE id_billetera = p_emisor;

    IF v_lim_dia IS NOT NULL THEN
        SELECT COALESCE(SUM(monto),0)
        INTO v_total_hoy
        FROM Transaccion_Yape ty
        JOIN Operacion_Yape oy ON ty.id_transaccion = oy.id_transaccion
        WHERE oy.id_billetera_emisor = p_emisor
          AND ty.estado = 'Exitosa'
          AND DATE(ty.fecha_hora) = CURRENT_DATE;

        IF v_total_hoy + p_monto > v_lim_dia THEN
            RAISE EXCEPTION 'Límite diario excedido.';
        END IF;
    END IF;

    -- ===============================
    -- 4. Obtener número receptor y nombres
    -- ===============================

    -- número_receptor = celular de billetera receptora
    SELECT celular
    INTO v_numero_receptor
    FROM Billetera_Yape
    WHERE id_billetera = p_receptor;

    -- nombre_receptor según tipo de billetera
    v_nombre_receptor := NULL;

    -- Receptor Persona
    SELECT COALESCE(p.correo, p.dni)
    INTO v_nombre_receptor
    FROM Billetera_Yape byp
    JOIN Persona p ON byp.id_actor = p.id_actor
    WHERE byp.id_billetera = p_receptor;

    IF NOT FOUND THEN
        -- Receptor Other_Persona
        SELECT nombre_externo
        INTO v_nombre_receptor
        FROM Billetera_Other_Persona bop
        WHERE bop.id_billetera = p_receptor;
    END IF;

    IF v_nombre_receptor IS NULL THEN
        -- Receptor Pequeño Negocio (Empresa_Pequeno_Negocio)
        SELECT e.nombre_comercial
        INTO v_nombre_receptor
        FROM Billetera_Empresa be
        JOIN Billetera_Yape byp ON be.id_billetera = byp.id_billetera
        JOIN Empresa_Pequeno_Negocio epn ON epn.id_actor = byp.id_actor
        JOIN Empresa e ON e.id_actor = epn.id_actor
        WHERE be.id_billetera = p_receptor;
    END IF;

    IF v_nombre_receptor IS NULL THEN
        v_nombre_receptor := 'Desconocido';
    END IF;

    -- nombre_emisor según tipo de billetera
    v_nombre_emisor := NULL;

    -- Emisor Persona
    SELECT COALESCE(p.correo, p.dni)
    INTO v_nombre_emisor
    FROM Billetera_Yape byp
    JOIN Persona p ON byp.id_actor = p.id_actor
    WHERE byp.id_billetera = p_emisor;

    IF NOT FOUND THEN
        -- Emisor Other_Persona
        SELECT nombre_externo
        INTO v_nombre_emisor
        FROM Billetera_Other_Persona bop
        WHERE bop.id_billetera = p_emisor;
    END IF;

    IF v_nombre_emisor IS NULL THEN
        -- Emisor Pequeño Negocio
        SELECT e.nombre_comercial
        INTO v_nombre_emisor
        FROM Billetera_Empresa be
        JOIN Billetera_Yape byp ON be.id_billetera = byp.id_billetera
        JOIN Empresa_Pequeno_Negocio epn ON epn.id_actor = byp.id_actor
        JOIN Empresa e ON e.id_actor = epn.id_actor
        WHERE be.id_billetera = p_emisor;
    END IF;

    IF v_nombre_emisor IS NULL THEN
        v_nombre_emisor := 'Desconocido';
    END IF;

    -- ===============================
    -- 5. Crear transacción principal
    -- ===============================
    INSERT INTO Transaccion_Yape (monto, estado, numero_operacion)
    VALUES (p_monto, 'Exitosa', v_num_op)
    RETURNING id_transaccion INTO v_tx_id;

    -- ===============================
    -- 6. Operación (emisor, receptor)
    -- ===============================
    INSERT INTO Operacion_Yape (id_transaccion, id_billetera_emisor, id_billetera_receptor)
    VALUES (v_tx_id, p_emisor, p_receptor);

    -- ===============================
    -- 7. Detalle Transaccion_Persona (completo)
    -- ===============================
    INSERT INTO Transaccion_Persona (
        id_transaccion,
        codigo_seguridad,
        tipo_destino,
        numero_receptor,
        nombre_receptor,
        nombre_emisor
    )
    VALUES (
        v_tx_id,
        p_codigo,
        p_tipo_destino,
        v_numero_receptor,
        v_nombre_receptor,
        v_nombre_emisor
    );

    -- ===============================
    -- 8. Actualizar saldos
    -- ===============================
    UPDATE Billetera_Yape
    SET saldo = saldo - p_monto
    WHERE id_billetera = p_emisor;

    UPDATE Billetera_Yape
    SET saldo = saldo + p_monto
    WHERE id_billetera = p_receptor;

    -- ===============================
    -- 9. Notificaciones (simple)
    -- ===============================
    INSERT INTO Notificacion (mensaje, estado, canal)
    VALUES (
        'Enviaste S/' || p_monto || ' a la billetera ' || p_receptor,
        'Enviada',
        'App'
    );

    INSERT INTO Notificacion (mensaje, estado, canal)
    VALUES (
        'Recibiste S/' || p_monto || ' de la billetera ' || p_emisor,
        'Enviada',
        'App'
    );

END;
$$;


----------------------------------------------------------------------
-- SP 2: PAGO DE SERVICIO
----------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE yape_pagar_servicio(
    p_emisor INT,
    p_receptor INT,
    p_monto DECIMAL(10,2),
    p_codigo_servicio VARCHAR(20)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_saldo DECIMAL(10,2);
    v_tx_id INT;
    v_num_op TEXT;
BEGIN
    -- Número operación OP + 15 dígitos
    v_num_op := fn_generar_numero_operacion();

    -- Validar saldo
    SELECT saldo INTO v_saldo
    FROM Billetera_Yape
    WHERE id_billetera = p_emisor
    FOR UPDATE;

    IF v_saldo < p_monto THEN
        RAISE EXCEPTION 'Saldo insuficiente.';
    END IF;

    -- Registrar transacción
    INSERT INTO Transaccion_Yape(monto, estado, numero_operacion)
    VALUES (p_monto, 'Exitosa', v_num_op)
    RETURNING id_transaccion INTO v_tx_id;

    INSERT INTO Operacion_Yape (id_transaccion, id_billetera_emisor, id_billetera_receptor)
    VALUES (v_tx_id, p_emisor, p_receptor);

    INSERT INTO Transaccion_Servicios(
        id_transaccion,
        codigo,
        descripcion,
        tipo_operacion_servicio
    )
    VALUES (
        v_tx_id,
        p_codigo_servicio,
        'Yapeaste S/' || p_monto || ' por servicio código ' || p_codigo_servicio,
        'PagoServicio'
    );

    -- Actualizar saldos
    UPDATE Billetera_Yape SET saldo = saldo - p_monto WHERE id_billetera = p_emisor;
    UPDATE Billetera_Yape SET saldo = saldo + p_monto WHERE id_billetera = p_receptor;

END;
$$;


----------------------------------------------------------------------
-- SP 3: PAGO AE (genera QR dinámico)
----------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE yape_pago_ae(
    p_emisor INT,
    p_receptor INT,
    p_monto DECIMAL(10,2),
    p_fecha_confirmacion TIMESTAMP
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_saldo DECIMAL(10,2);
    v_tx INT;
    v_qr INT;
    v_num_op TEXT;
    v_codigo_qr TEXT;
BEGIN
    -- Número operación OP + 15 dígitos
    v_num_op := fn_generar_numero_operacion();

    -- Validar saldo
    SELECT saldo INTO v_saldo
    FROM Billetera_Yape
    WHERE id_billetera = p_emisor
    FOR UPDATE;

    IF v_saldo < p_monto THEN
        RAISE EXCEPTION 'Saldo insuficiente.';
    END IF;

    -- Crear transacción
    INSERT INTO Transaccion_Yape(monto, estado, numero_operacion)
    VALUES(p_monto, 'Pendiente', v_num_op)
    RETURNING id_transaccion INTO v_tx;

    INSERT INTO Operacion_Yape (id_transaccion, id_billetera_emisor, id_billetera_receptor)
    VALUES(v_tx, p_emisor, p_receptor);

    -- Crear QR Dinámico con UUID
    v_codigo_qr := gen_random_uuid()::text;

    INSERT INTO QR(codigo_qr, activo)
    VALUES(v_codigo_qr, TRUE)
    RETURNING id_qr INTO v_qr;

    INSERT INTO QR_Dinamico(id_qr, monto_fijo, tiempo_validez)
    VALUES(v_qr, p_monto, 300);

    -- Insert en Transaccion_Empresa_AE
    INSERT INTO Transaccion_Empresa_AE(
        id_transaccion,
        id_qr,
        fecha_confirmacion,
        descripcion
    )
    VALUES(
        v_tx,
        v_qr,
        p_fecha_confirmacion,
        'Pago QR generado por monto S/' || p_monto
    );

END;
$$;


----------------------------------------------------------------------
-- SP 4: CONFIRMAR PAGO AE
----------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE yape_confirmar_ae(
    p_transaccion INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_monto DECIMAL(10,2);
    v_e INT;
    v_r INT;
BEGIN
    SELECT monto INTO v_monto
    FROM Transaccion_Yape
    WHERE id_transaccion = p_transaccion;

    SELECT id_billetera_emisor, id_billetera_receptor
    INTO v_e, v_r
    FROM Operacion_Yape
    WHERE id_transaccion = p_transaccion;

    UPDATE Billetera_Yape SET saldo = saldo - v_monto WHERE id_billetera = v_e;
    UPDATE Billetera_Yape SET saldo = saldo + v_monto WHERE id_billetera = v_r;

    UPDATE Transaccion_Yape
    SET estado = 'Exitosa'
    WHERE id_transaccion = p_transaccion;

    UPDATE Transaccion_Empresa_AE
    SET fecha_confirmacion = NOW()
    WHERE id_transaccion = p_transaccion;
END;
$$;


/* ===============================================================
               FIN DEL ARCHIVO COMPLETO
=============================================================== */