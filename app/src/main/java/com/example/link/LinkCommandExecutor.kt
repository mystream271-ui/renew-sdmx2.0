package com.example.link

import android.content.Context
import com.example.data.AppLocks
import com.example.data.LocalDatabase
import com.example.data.LogManager
import com.example.data.PreferencesManager
import com.example.data.UserModel
import com.example.network.SdmxApiService
import com.example.notifications.NtfyManager
import com.example.worker.SdmxExecutionEngine
import com.example.worker.SdmxForegroundService
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.Locale
import kotlin.math.ceil

object LinkCommandExecutor {

    private val rateLimiter = RateLimiter(maxPerMinute = 30)
    private val api = SdmxApiService()
    private val gson = Gson()
    private val inFlightCommandIds = java.util.Collections.synchronizedSet(mutableSetOf<String>())

    /**
     * Enmascara contraseñas para logs y reportes: ej. "Ab123456" -> "Ab******"
     */
    fun maskPassword(pass: String): String {
        if (pass.isEmpty()) return ""
        return if (pass.length <= 2) "**" else pass.take(2) + "*".repeat(pass.length - 2)
    }

    /**
     * Procesa un sobre de comando deserializado.
     */
    suspend fun processEnvelope(context: Context, envelope: LinkEnvelope): LinkEnvelope? {
        return handleEnvelope(context, envelope)
    }

    /**
     * Procesa un mensaje de comando JSON en crudo entrante.
     */
    suspend fun handleMessage(context: Context, rawMessage: String): LinkEnvelope? = withContext(Dispatchers.IO) {
        val envelope = LinkEnvelope.fromJson(rawMessage.trim()) ?: return@withContext null
        if (envelope.cmd.isNullOrBlank()) {
            return@withContext null
        }
        return@withContext handleEnvelope(context, envelope)
    }

    /**
     * Ejecuta el orden estricto de validación según el CONTRATO v1:
     * 1. Firma -> 2. TTL -> 3. Id repetido (idempotencia) -> 4. Rate limit -> 5. Payload y ejecución.
     */
    private suspend fun handleEnvelope(context: Context, envelope: LinkEnvelope): LinkEnvelope = withContext(Dispatchers.IO) {
        val secret = PreferencesManager.getLinkSecret(context)
        val respTopic = PreferencesManager.getLinkTopicResp(context)
        val processedStore = ProcessedStore(context)

        // 1. Verificación de firma HMAC-SHA256
        if (!LinkProtocol.verify(envelope, secret)) {
            val resp = LinkProtocol.buildResponse(
                re = envelope.id,
                ok = false,
                code = LinkCodes.BAD_SIGNATURE,
                msg = "Firma HMAC-SHA256 inválida o secreta incorrecta",
                data = null,
                secret = secret
            )
            processedStore.put(envelope.id, resp)
            NtfyManager.publish(context, respTopic, resp.toJson(), priority = 3)
            LogManager.addLog(context, "❌ [Enlace Bot] Firma rechazada para comando '${envelope.cmd}' (ID ${envelope.id})")
            return@withContext resp
        }

        // 2. Verificación de frescura / TTL
        if (!LinkProtocol.isFresh(envelope.cmd, envelope.ts)) {
            val resp = LinkProtocol.buildResponse(
                re = envelope.id,
                ok = false,
                code = LinkCodes.EXPIRED,
                msg = "Comando expirado por límite de tiempo (TTL)",
                data = null,
                secret = secret
            )
            processedStore.put(envelope.id, resp)
            NtfyManager.publish(context, respTopic, resp.toJson(), priority = 3)
            LogManager.addLog(context, "⏳ [Enlace Bot] Comando '${envelope.cmd}' expirado (ts=${envelope.ts})")
            return@withContext resp
        }

        // 3. Idempotencia y verificación de comandos en curso
        val cached = processedStore.get(envelope.id)
        if (cached != null) {
            LogManager.addLog(context, "🔁 [Enlace Bot] Comando repetido '${envelope.cmd}' (ID ${envelope.id}). Reenviando respuesta en caché.")
            NtfyManager.publish(context, respTopic, cached.toJson(), priority = 3)
            return@withContext cached
        }

        if (inFlightCommandIds.contains(envelope.id)) {
            LogManager.addLog(context, "⏳ [Enlace Bot] Comando '${envelope.cmd}' (ID ${envelope.id}) ya se encuentra en proceso. Evitando reencolado duplicado.")
            val inFlightResp = LinkProtocol.buildResponse(
                re = envelope.id,
                ok = true,
                code = LinkCodes.QUEUED,
                msg = "Comando ya en proceso de ejecución",
                data = null,
                secret = secret
            )
            return@withContext inFlightResp
        }

        // 4. Rate Limiter (máximo 30 comandos de escritura por minuto)
        val isWriteCommand = envelope.cmd in listOf("ADD_USER", "EXTEND_USER", "DELETE_USER")
        if (isWriteCommand && !rateLimiter.tryAcquire()) {
            val resp = LinkProtocol.buildResponse(
                re = envelope.id,
                ok = false,
                code = LinkCodes.RATE_LIMITED,
                msg = "Límite de tasa excedido (máximo 30 comandos por minuto)",
                data = null,
                secret = secret
            )
            processedStore.put(envelope.id, resp)
            NtfyManager.publish(context, respTopic, resp.toJson(), priority = 3)
            LogManager.addLog(context, "⚠️ [Enlace Bot] Comando '${envelope.cmd}' bloqueado por RATE_LIMITED")
            return@withContext resp
        }

        // 5. Verificación de integridad de la base de datos local
        val db = LocalDatabase.get(context)
        if (isWriteCommand && db.hasLoadError.value) {
            val resp = LinkProtocol.buildResponse(
                re = envelope.id,
                ok = false,
                code = LinkCodes.INTERNAL,
                msg = "Base de datos local dañada; operaciones de escritura suspendidas",
                data = null,
                secret = secret
            )
            processedStore.put(envelope.id, resp)
            NtfyManager.publish(context, respTopic, resp.toJson(), priority = 3)
            LogManager.addLog(context, "🛑 [Enlace Bot] Escritura suspendida: base de datos local con error de carga.")
            return@withContext resp
        }

        // 6. Si hay un ciclo de renovación en curso, responder de inmediato QUEUED y encolar ejecución
        if (isWriteCommand && SdmxExecutionEngine.executionMutex.isLocked) {
            inFlightCommandIds.add(envelope.id)
            val queuedResp = LinkProtocol.buildResponse(
                re = envelope.id,
                ok = true,
                code = LinkCodes.QUEUED,
                msg = "Ciclo de renovación en curso; comando encolado para ejecución posterior",
                data = null,
                secret = secret
            )
            processedStore.put(envelope.id, queuedResp)
            NtfyManager.publish(context, respTopic, queuedResp.toJson(), priority = 3)
            LogManager.addLog(context, "⏳ [Enlace Bot] Ciclo en curso. Respondido QUEUED para '${envelope.cmd}' (ID ${envelope.id}).")

            CoroutineScope(Dispatchers.IO).launch {
                try {
                    val finalResponse = executeCommand(context, envelope, secret, db)
                    processedStore.put(envelope.id, finalResponse)
                    NtfyManager.publish(context, respTopic, finalResponse.toJson(), priority = 3)
                    LogManager.addLog(context, "✅ [Enlace Bot] Comando encolado '${envelope.cmd}' completado (ID ${envelope.id})")
                } finally {
                    inFlightCommandIds.remove(envelope.id)
                }
            }
            return@withContext queuedResp
        }

        // 7. Ejecución inmediata (PING, LIST_USERS y comandos cuando no hay ciclo activo)
        LogManager.addLog(context, "⚡ [Enlace Bot] Procesando '${envelope.cmd}' (ID ${envelope.id})...")
        inFlightCommandIds.add(envelope.id)
        val response = try {
            executeCommand(context, envelope, secret, db)
        } finally {
            inFlightCommandIds.remove(envelope.id)
        }

        // Guardar en store para idempotencia y publicar en RESP
        processedStore.put(envelope.id, response)
        NtfyManager.publish(context, respTopic, response.toJson(), priority = 3)
        return@withContext response
    }

    private suspend fun executeCommand(
        context: Context,
        envelope: LinkEnvelope,
        secret: String,
        db: LocalDatabase
    ): LinkEnvelope {
        return when (envelope.cmd) {
            "PING" -> executePing(context, envelope, secret, db)
            "LIST_USERS" -> executeListUsers(envelope, secret, db)
            "ADD_USER" -> executeAddUser(context, envelope, secret, db)
            "EXTEND_USER" -> executeExtendUser(context, envelope, secret, db)
            "DELETE_USER" -> executeDeleteUser(context, envelope, secret, db)
            else -> {
                LinkProtocol.buildResponse(
                    re = envelope.id,
                    ok = false,
                    code = LinkCodes.BAD_REQUEST,
                    msg = "Comando desconocido: ${envelope.cmd}",
                    data = null,
                    secret = secret
                )
            }
        }
    }

    private suspend fun executePing(
        context: Context,
        envelope: LinkEnvelope,
        secret: String,
        db: LocalDatabase
    ): LinkEnvelope {
        val users = db.getUsers()
        val vigentesCount = users.count { it.isVigente() }
        val lastRun = PreferencesManager.getSyncLastRunTime(context)
        val nextRun = PreferencesManager.getSyncNextRunTime(context)
        val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)

        val data = mapOf(
            "app" to "Sdmx_Renew",
            "usuarios_total" to users.size,
            "vigentes" to vigentesCount,
            "ultimo_ciclo" to if (lastRun > 0) sdf.format(Date(lastRun)) else "Nunca",
            "proximo_ciclo" to if (nextRun > 0) sdf.format(Date(nextRun)) else "Pendiente",
            "ciclo_en_curso" to SdmxExecutionEngine.executionMutex.isLocked
        )

        return LinkProtocol.buildResponse(
            re = envelope.id,
            ok = true,
            code = LinkCodes.OK,
            msg = "Pong",
            data = data,
            secret = secret
        )
    }

    private suspend fun executeListUsers(
        envelope: LinkEnvelope,
        secret: String,
        db: LocalDatabase
    ): LinkEnvelope {
        val payloadObj = try {
            JsonParser.parseString(envelope.payload).asJsonObject
        } catch (_: Exception) {
            JsonObject()
        }

        val pagina = if (payloadObj.has("pagina")) payloadObj.get("pagina").asInt else 1
        val estadoFiltro = payloadObj.get("estado")?.asString?.trim()?.lowercase() ?: "todos"
        val creadoPorFiltro = payloadObj.get("creado_por")?.asString?.trim()
        val buscarFiltro = payloadObj.get("buscar")?.asString?.trim()

        val allUsers = db.getUsers()

        // 1. Aplicación de filtros
        val filteredUsers = allUsers.filter { u ->
            val matchesEstado = when (estadoFiltro) {
                "vigentes" -> u.isVigente()
                "vencidos" -> !u.isVigente()
                else -> true // "todos"
            }
            val matchesCreadoPor = if (!creadoPorFiltro.isNullOrEmpty()) {
                (u.creado_por ?: "").equals(creadoPorFiltro, ignoreCase = true)
            } else true

            val matchesBuscar = if (!buscarFiltro.isNullOrEmpty()) {
                u.usuario.contains(buscarFiltro, ignoreCase = true)
            } else true

            matchesEstado && matchesCreadoPor && matchesBuscar
        }

        // 2. Ordenamiento: por vencimiento ascendente (los más próximos a vencer primero)
        val sortedUsers = filteredUsers.sortedWith(compareBy(
            {
                val clean = it.vencimiento.trim().substringBefore("T")
                if (clean.isEmpty()) "9999-99-99" else clean
            },
            { it.usuario.lowercase() }
        ))

        // 3. Mapeo a items de respuesta
        val allMappedItems = sortedUsers.map { u ->
            mapOf(
                "id" to u.id,
                "usuario" to u.usuario,
                "password" to u.password,
                "vence" to u.vencimiento,
                "adultos" to u.adultos,
                "estado" to u.getEstado(),
                "dias_restantes" to u.getDiasRestantes(),
                "origen" to u.origen,
                "telefono" to (u.telefono ?: ""),
                "creado_por" to (u.creado_por ?: "")
            )
        }

        // 4. Paginación estricta por tamaño de sobre UTF-8 (<= 3500 bytes por página)
        val pages = mutableListOf<List<Map<String, Any>>>()
        if (allMappedItems.isEmpty()) {
            pages.add(emptyList())
        } else {
            var currentChunk = mutableListOf<Map<String, Any>>()
            for (item in allMappedItems) {
                currentChunk.add(item)
                val testData = mapOf(
                    "pagina" to 1,
                    "paginas" to 1,
                    "total" to allMappedItems.size,
                    "items" to currentChunk
                )
                val testEnvelope = LinkProtocol.buildResponse(
                    re = envelope.id,
                    ok = true,
                    code = LinkCodes.OK,
                    msg = "Usuarios listados",
                    data = testData,
                    secret = secret
                )
                val byteSize = testEnvelope.toJson().toByteArray(Charsets.UTF_8).size
                if (byteSize > 3500) {
                    if (currentChunk.size > 1) {
                        currentChunk.removeAt(currentChunk.size - 1)
                        pages.add(ArrayList(currentChunk))
                        currentChunk = mutableListOf(item)
                    } else {
                        pages.add(ArrayList(currentChunk))
                        currentChunk = mutableListOf()
                    }
                }
            }
            if (currentChunk.isNotEmpty()) {
                pages.add(currentChunk)
            }
        }

        val totalPages = pages.size

        // 5. Validación de rango de página
        if (pagina < 1 || pagina > totalPages) {
            return LinkProtocol.buildResponse(
                re = envelope.id,
                ok = false,
                code = LinkCodes.BAD_REQUEST,
                msg = "Página $pagina fuera de rango (1..$totalPages)",
                data = null,
                secret = secret
            )
        }

        val pageItems = pages[pagina - 1]
        val data = mapOf(
            "pagina" to pagina,
            "paginas" to totalPages,
            "total" to allMappedItems.size,
            "items" to pageItems
        )

        return LinkProtocol.buildResponse(
            re = envelope.id,
            ok = true,
            code = LinkCodes.OK,
            msg = "Usuarios listados",
            data = data,
            secret = secret
        )
    }

    private suspend fun executeAddUser(
        context: Context,
        envelope: LinkEnvelope,
        secret: String,
        db: LocalDatabase
    ): LinkEnvelope {
        val payloadObj = try {
            JsonParser.parseString(envelope.payload).asJsonObject
        } catch (e: Exception) {
            return LinkProtocol.buildResponse(envelope.id, false, LinkCodes.BAD_REQUEST, "Payload JSON inválido", null, secret)
        }

        val usuario = payloadObj.get("usuario")?.asString?.trim() ?: ""
        val password = payloadObj.get("password")?.asString ?: ""
        val vence = payloadObj.get("vence")?.asString?.trim() ?: ""
        val modo = payloadObj.get("modo")?.asString ?: "crear_en_panel"
        val adultos = payloadObj.get("adultos")?.asBoolean ?: false
        val origen = payloadObj.get("origen")?.asString?.trim()?.ifEmpty { "manual" } ?: "manual"
        val telefono = payloadObj.get("telefono")?.asString?.trim()
        val creado_por = payloadObj.get("creado_por")?.asString?.trim()
        val reemplaza = payloadObj.get("reemplaza")?.asString?.trim()

        // Validaciones de contrato
        val userVal = LinkProtocol.validateUsername(usuario, origen)
        if (!userVal.isValid) {
            return LinkProtocol.buildResponse(envelope.id, false, userVal.errorCode ?: LinkCodes.INVALID_USERNAME, userVal.errorMessage ?: "Usuario inválido", null, secret)
        }

        val passVal = LinkProtocol.validatePassword(password)
        if (!passVal.isValid) {
            return LinkProtocol.buildResponse(envelope.id, false, passVal.errorCode ?: LinkCodes.INVALID_PASSWORD, passVal.errorMessage ?: "Contraseña inválida", null, secret)
        }

        val dateVal = LinkProtocol.validateDate(vence)
        if (!dateVal.isValid) {
            return LinkProtocol.buildResponse(envelope.id, false, dateVal.errorCode ?: LinkCodes.INVALID_DATE, dateVal.errorMessage ?: "Fecha inválida", null, secret)
        }

        // Manejo de duplicados: si coincide exactamente -> OK ya_existia:true; si difiere -> DUPLICATE_USER
        val existing = AppLocks.data.withLock { db.findUser(usuario) }
        if (existing != null) {
            val samePass = existing.password == password
            val sameDate = existing.vencimiento.trim() == vence.trim()
            val sameAdultos = existing.adultos == adultos
            if (samePass && sameDate && sameAdultos) {
                val data = mapOf(
                    "id" to existing.id,
                    "usuario" to existing.usuario,
                    "vence" to existing.vencimiento,
                    "estado" to existing.getEstado(),
                    "ya_existia" to true
                )
                return LinkProtocol.buildResponse(envelope.id, true, LinkCodes.OK, "Usuario ya existía con idénticos parámetros", data, secret)
            } else {
                return LinkProtocol.buildResponse(
                    envelope.id,
                    false,
                    LinkCodes.DUPLICATE_USER,
                    "El usuario '$usuario' ya existe con datos distintos",
                    null,
                    secret
                )
            }
        }

        // Adquisición estricta de locks: executionMutex primero para operaciones sobre panel
        return SdmxExecutionEngine.executionMutex.withLock {
            var lineId = ""

            val (adminUser, adminPass) = PreferencesManager.getSyncCredentials(context)
            if (adminUser.isNotEmpty() && adminPass.isNotEmpty()) {
                val loginOk = api.login(context, adminUser, adminPass)
                if (!loginOk) {
                    return@withLock LinkProtocol.buildResponse(
                        envelope.id,
                        false,
                        LinkCodes.PANEL_LOGIN_FAILED,
                        "Fallo de inicio de sesión en el panel SDMX",
                        null,
                        secret
                    )
                }

                if (modo == "registrar_existente") {
                    var attempts = 0
                    while (attempts < 3 && lineId.isEmpty()) {
                        val tableIds = api.getTableIds(context)
                        lineId = tableIds[usuario.lowercase()] ?: tableIds[usuario] ?: ""
                        if (lineId.isEmpty()) {
                            attempts++
                            if (attempts < 3) delay(2000)
                        }
                    }

                    if (lineId.isEmpty()) {
                        LogManager.addLog(context, "❌ [Enlace Bot] ID no localizado en panel para '$usuario' tras 3 reintentos.")
                        return@withLock LinkProtocol.buildResponse(
                            envelope.id,
                            false,
                            LinkCodes.ID_NOT_FOUND,
                            "Línea '$usuario' no encontrada en tabla SDMX tras reintentos",
                            null,
                            secret
                        )
                    }
                } else {
                    // Modo crear_en_panel
                    val createRes = api.createLine(context, usuario, password, vence, adultos)
                    if (createRes.isFailure) {
                        val errMsg = createRes.exceptionOrNull()?.message ?: "Panel rechazó la creación"
                        return@withLock LinkProtocol.buildResponse(
                            envelope.id,
                            false,
                            LinkCodes.PANEL_CREATE_FAILED,
                            "Error al crear en panel SDMX: $errMsg",
                            null,
                            secret
                        )
                    }

                    delay(1500)
                    var attempts = 0
                    while (attempts < 3 && lineId.isEmpty()) {
                        val tableIds = api.getTableIds(context)
                        lineId = tableIds[usuario.lowercase()] ?: tableIds[usuario] ?: ""
                        if (lineId.isEmpty()) {
                            attempts++
                            if (attempts < 3) delay(2000)
                        }
                    }
                }
            } else if (modo == "crear_en_panel") {
                return@withLock LinkProtocol.buildResponse(
                    envelope.id,
                    false,
                    LinkCodes.PANEL_LOGIN_FAILED,
                    "Credenciales de administrador SDMX no configuradas",
                    null,
                    secret
                )
            }

            // Construir UserModel
            val newUser = UserModel(
                id = lineId,
                usuario = usuario,
                password = password,
                vencimiento = vence,
                adultos = adultos,
                origen = origen,
                telefono = telefono,
                creado_por = creado_por,
                notas = if (!reemplaza.isNullOrBlank()) "Reemplaza a $reemplaza" else null,
                creadoEn = System.currentTimeMillis()
            )

            // Inserción atómica en la base de datos protegida por AppLocks.data
            val added = AppLocks.data.withLock { db.addIfAbsent(newUser) }
            if (!added) {
                return@withLock LinkProtocol.buildResponse(
                    envelope.id,
                    false,
                    LinkCodes.DUPLICATE_USER,
                    "El usuario '$usuario' ya existía al momento de insertar",
                    null,
                    secret
                )
            }

            LogManager.addLog(
                context,
                "✅ [Enlace Bot] Usuario registrado: $usuario | pass: ${maskPassword(password)} | vence: $vence | id: #$lineId"
            )

            // Reemplazo opcional: SOLO después de agregar con éxito se elimina la cuenta reemplazada
            var reemplazoStatus = "no_aplica"
            if (!reemplaza.isNullOrBlank()) {
                try {
                    LogManager.addLog(context, "🔄 [Enlace Bot] Procesando reemplazo de '$reemplaza'...")
                    api.deleteUserFromPanel(context, adminUser, adminPass, reemplaza)
                    AppLocks.data.withLock {
                        db.removeUser(reemplaza)
                    }
                    reemplazoStatus = "OK"
                    LogManager.addLog(context, "✅ [Enlace Bot] Cuenta previa '$reemplaza' eliminada correctamente.")
                } catch (e: Exception) {
                    reemplazoStatus = "Error: ${e.message}"
                    LogManager.addLog(context, "⚠️ [Enlace Bot] No se pudo eliminar reemplazo '$reemplaza': ${e.message}")
                }
            }

            // Refresco de notificación persistente y estado
            val nextRun = PreferencesManager.getSyncNextRunTime(context)
            val vigentesCount = db.getUsers().count { it.isVigente() }
            SdmxForegroundService.updateNotificationStatus(context, nextRun, vigentesCount)

            val data = mutableMapOf<String, Any>(
                "id" to newUser.id,
                "usuario" to newUser.usuario,
                "vence" to newUser.vencimiento,
                "estado" to newUser.getEstado(),
                "ya_existia" to false
            )
            if (!reemplaza.isNullOrBlank()) {
                data["reemplazo"] = reemplazoStatus
            }

            LinkProtocol.buildResponse(
                re = envelope.id,
                ok = true,
                code = LinkCodes.OK,
                msg = "Usuario registrado exitosamente",
                data = data,
                secret = secret
            )
        }
    }

    private suspend fun executeExtendUser(
        context: Context,
        envelope: LinkEnvelope,
        secret: String,
        db: LocalDatabase
    ): LinkEnvelope {
        val payloadObj = try {
            JsonParser.parseString(envelope.payload).asJsonObject
        } catch (e: Exception) {
            return LinkProtocol.buildResponse(envelope.id, false, LinkCodes.BAD_REQUEST, "Payload JSON inválido", null, secret)
        }

        val usuario = payloadObj.get("usuario")?.asString?.trim() ?: ""
        val base = payloadObj.get("base")?.asString ?: "desde_vencimiento"
        val dias = payloadObj.get("dias")?.asInt ?: 0
        val meses = payloadObj.get("meses")?.asInt ?: 0

        if (usuario.isEmpty()) {
            return LinkProtocol.buildResponse(envelope.id, false, LinkCodes.BAD_REQUEST, "El parámetro 'usuario' es obligatorio", null, secret)
        }

        if (dias <= 0 && meses <= 0) {
            return LinkProtocol.buildResponse(envelope.id, false, LinkCodes.BAD_REQUEST, "Se debe especificar días o meses mayores a 0 para extender vigencia", null, secret)
        }

        return SdmxExecutionEngine.executionMutex.withLock {
            val user = AppLocks.data.withLock { db.findUser(usuario) }
            if (user == null) {
                return@withLock LinkProtocol.buildResponse(
                    envelope.id,
                    false,
                    LinkCodes.NOT_FOUND,
                    "Usuario '$usuario' no encontrado en la base local",
                    null,
                    secret
                )
            }

            val sdf = SimpleDateFormat("yyyy-MM-dd", Locale.US)
            val cal = Calendar.getInstance().apply {
                set(Calendar.HOUR_OF_DAY, 0)
                set(Calendar.MINUTE, 0)
                set(Calendar.SECOND, 0)
                set(Calendar.MILLISECOND, 0)
            }
            val venceAnterior = user.vencimiento.trim().substringBefore("T")

            // Si base es desde_vencimiento y la cuenta está vigente, sumar a partir de la fecha de vencimiento actual.
            // Si está vencida, sumar a partir de hoy.
            if (base == "desde_vencimiento" && user.isVigente()) {
                try {
                    val prevDate = sdf.parse(venceAnterior)
                    if (prevDate != null) cal.time = prevDate
                } catch (_: Exception) {}
            }

            if (meses > 0) cal.add(Calendar.MONTH, meses)
            if (dias > 0) cal.add(Calendar.DAY_OF_YEAR, dias)

            val venceNueva = sdf.format(cal.time)
            val updatedUser = user.copy(vencimiento = venceNueva)

            AppLocks.data.withLock {
                db.addUser(updatedUser)
            }

            LogManager.addLog(context, "📅 [Enlace Bot] Vigencia extendida para '$usuario': $venceAnterior → $venceNueva")

            // Refresco de notificación persistente
            val nextRun = PreferencesManager.getSyncNextRunTime(context)
            val vigentesCount = db.getUsers().count { it.isVigente() }
            SdmxForegroundService.updateNotificationStatus(context, nextRun, vigentesCount)

            val data = mapOf(
                "usuario" to usuario,
                "vence_anterior" to venceAnterior,
                "vence_nueva" to venceNueva
            )

            LinkProtocol.buildResponse(
                re = envelope.id,
                ok = true,
                code = LinkCodes.OK,
                msg = "Vigencia extendida exitosamente",
                data = data,
                secret = secret
            )
        }
    }

    private suspend fun executeDeleteUser(
        context: Context,
        envelope: LinkEnvelope,
        secret: String,
        db: LocalDatabase
    ): LinkEnvelope {
        val payloadObj = try {
            JsonParser.parseString(envelope.payload).asJsonObject
        } catch (e: Exception) {
            return LinkProtocol.buildResponse(envelope.id, false, LinkCodes.BAD_REQUEST, "Payload JSON inválido", null, secret)
        }

        val usuario = payloadObj.get("usuario")?.asString?.trim() ?: ""
        if (usuario.isEmpty()) {
            return LinkProtocol.buildResponse(envelope.id, false, LinkCodes.BAD_REQUEST, "El parámetro 'usuario' es obligatorio", null, secret)
        }

        // Adquisición de executionMutex
        return SdmxExecutionEngine.executionMutex.withLock {
            val user = AppLocks.data.withLock { db.findUser(usuario) }
            if (user == null) {
                return@withLock LinkProtocol.buildResponse(
                    envelope.id,
                    false,
                    LinkCodes.NOT_FOUND,
                    "Usuario '$usuario' no encontrado en la app",
                    null,
                    secret
                )
            }

            // 1. Panel primero: eliminar línea en SDMX fuera de AppLocks.data
            val (adminUser, adminPass) = PreferencesManager.getSyncCredentials(context)
            if (adminUser.isBlank() || adminPass.isBlank()) {
                LogManager.addLog(context, "❌ [Enlace Bot] No se puede eliminar '$usuario': Credenciales SDMX no configuradas.")
                return@withLock LinkProtocol.buildResponse(
                    envelope.id,
                    false,
                    LinkCodes.PANEL_LOGIN_FAILED,
                    "Credenciales de administrador SDMX no configuradas",
                    null,
                    secret
                )
            }

            val deleteResult = api.deleteUserFromPanel(context, adminUser, adminPass, user.usuario, user.id)
            if (deleteResult.isFailure) {
                val errMsg = deleteResult.exceptionOrNull()?.message ?: "Error desconocido en panel"
                val errorCode = if (errMsg.contains("sesión", ignoreCase = true) || errMsg.contains("Credenciales", ignoreCase = true)) {
                    LinkCodes.PANEL_LOGIN_FAILED
                } else {
                    LinkCodes.PANEL_DELETE_FAILED
                }
                LogManager.addLog(context, "❌ [Enlace Bot] Fallo al eliminar '$usuario' en panel: $errMsg. No se borra de la base local.")
                return@withLock LinkProtocol.buildResponse(
                    envelope.id,
                    false,
                    errorCode,
                    "Error al eliminar del panel SDMX: $errMsg",
                    null,
                    secret
                )
            }

            // 2. Base después: eliminar de la base de datos local bajo AppLocks.data SOLO si la baja en panel fue exitosa
            AppLocks.data.withLock {
                db.removeUser(user.usuario)
            }
            LogManager.addLog(context, "🗑️ [Enlace Bot] Usuario eliminado (panel primero, base después): $usuario")

            // Refresco de notificación persistente
            val nextRun = PreferencesManager.getSyncNextRunTime(context)
            val vigentesCount = db.getUsers().count { it.isVigente() }
            SdmxForegroundService.updateNotificationStatus(context, nextRun, vigentesCount)

            val data = mapOf("usuario" to user.usuario)

            LinkProtocol.buildResponse(
                re = envelope.id,
                ok = true,
                code = LinkCodes.OK,
                msg = "Usuario eliminado exitosamente",
                data = data,
                secret = secret
            )
        }
    }
}
