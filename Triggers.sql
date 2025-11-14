
-- Triggers

--  Persona, Empresa, Empresa_Pequeno_Negocio, Empresa_Servicios,
--  Empresa_Acceso_Empresarial, Funcionalidad

-- No hay triggers, las restricciones impuestas son suficientes


-- Billetera_Yape

-- Una Billetera_Yape asociada a un Actor_Yape siempre debe tener origen 'Yape'
-- Celular solo debe ser no nulo y único
-- para Billetera Persona, Billetera_Other_Persona, Billetera_Empresa asociada a
-- Empresa_Pequeno_Negocio
-- Billetera_Persona solo asociado a Persona
-- Billetera_Empresa solo asociada a Empresa
-- Billetera_Other_Persona no está asociada a ningún Actor_Yape


-- Credencial

-- Solo puede haber una credencial activa por cada tipo
-- Cada vez que se agrega una credencial de un tipo, la credencial actual del mismo tipo
-- debe pasar a expirada

-- QR

-- QR_Estatico debe ser creado al mismo tiempo que se crea una Billetera_Persona o
-- cuando se crea una Billetera_Empresa asociada a una Empresa_Pequeno_Negocio

-- QR_Dinamico debe crearse cuando se crea una Transaccion_Empresa_AE con el mismo monto
-- asociado a la Transaccion


-- Transaccion

-- Cada Transaccion exitosa debe reflejar los cambios en
-- el saldo del emisor como receptor(excepción para Billetera_Other_Persona)

-- Transaccion_Persona debe contener un número de celular existente de alguna
-- Billetera(Billetera_Persona, Billetera_Other_Persona, Billetera_Empresa asociada a Empresa_Pequeno_Negocio)
-- Transaccion_Servicio debe tener como emisor a Billetera_Persona y receptor a
-- Billetera_Empresa asociada a Empresa_Servicios
-- Transaccion_Empresa_AE debe tener como emisor a Billetera_Persona y receptor a
-- Billetera_Empresa asociada a Empresa_Acceso_Empresarial
-- Transaccion_Empresa_AE fecha_confirmacion debe ser posterior a la fecha_hora de la Transaccion

-- Notificacion

-- Notificacion_Transaccion debe crearse cuando una Transacción es Exitosa o Fallida para el emisor y receptor
-- Notificacion_Transaccion debe dar el detalle de la Transaccion en el mensaje
