package com.example.link

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import com.example.data.LocalDatabase
import com.example.data.PreferencesManager
import com.example.data.UserModel
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.io.File
import java.util.UUID

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [36])
class LinkCommandExecutorTest {

    private lateinit var context: Context
    private val secret = "12345678901234567890123456789012" // 32 chars
    private val gson = Gson()

    @Before
    fun setUp() {
        context = ApplicationProvider.getApplicationContext()
        LocalDatabase.resetForTesting()
        val file = File(context.filesDir, "sdmx_users.json")
        val tmpFile = File(context.filesDir, "sdmx_users.json.tmp")
        val bakFile = File(context.filesDir, "sdmx_users.json.bak")
        val processedFile = File(context.filesDir, ProcessedStore.DEFAULT_FILE_NAME)
        if (file.exists()) file.delete()
        if (tmpFile.exists()) tmpFile.delete()
        if (bakFile.exists()) bakFile.delete()
        if (processedFile.exists()) processedFile.delete()

        PreferencesManager.setSyncLinkConfig(
            context = context,
            enabled = true,
            baseUrl = "https://ntfy.sh",
            topicCmd = "SDMX_RENEW_CMD",
            topicResp = "SDMX_RENEW_RESP",
            secret = secret,
            token = ""
        )
    }

    private fun createSignedEnvelope(cmd: String, payload: String, id: String = UUID.randomUUID().toString(), ts: Long = System.currentTimeMillis() / 1000L): LinkEnvelope {
        val signable = "1|$id|$ts|$cmd|$payload"
        val sig = LinkProtocol.sign(secret, signable)
        return LinkEnvelope(v = 1, id = id, ts = ts, cmd = cmd, payload = payload, sig = sig)
    }

    @Test
    fun testPingCommand() = runBlocking {
        val db = LocalDatabase.get(context)
        db.saveUsers(listOf(UserModel(id = "1", usuario = "user1", vencimiento = "2028-12-31")))

        val envelope = createSignedEnvelope("PING", "{}")
        val response = LinkCommandExecutor.handleMessage(context, envelope.toJson())

        assertNotNull(response)
        val payload = LinkResponsePayload.fromJson(response!!.payload)
        assertNotNull(payload)
        assertTrue(payload!!.ok)
        assertEquals(LinkCodes.OK, payload.code)
        assertEquals("Pong", payload.msg)

        val data = gson.toJsonTree(payload.data).asJsonObject
        assertEquals("Sdmx_Renew", data.get("app").asString)
        assertEquals(1, data.get("usuarios_total").asInt)
        assertEquals(1, data.get("vigentes").asInt)
    }

    @Test
    fun testListUsersCommand() = runBlocking {
        val db = LocalDatabase.get(context)
        db.saveUsers(listOf(
            UserModel(id = "101", usuario = "juan_perez", password = "SecretPassword1", vencimiento = "2027-03-01", origen = "vip", telefono = "1234567890", creado_por = "admin_bot")
        ))

        val envelope = createSignedEnvelope("LIST_USERS", """{"pagina":1,"limite":10}""")
        val response = LinkCommandExecutor.handleMessage(context, envelope.toJson())

        assertNotNull(response)
        val payload = LinkResponsePayload.fromJson(response!!.payload)
        assertTrue(payload!!.ok)
        val data = gson.toJsonTree(payload.data).asJsonObject
        assertEquals(1, data.get("total").asInt)
        val items = data.getAsJsonArray("items")
        assertEquals(1, items.size())
        val item = items.get(0).asJsonObject
        assertEquals("juan_perez", item.get("usuario").asString)
        assertEquals("vip", item.get("origen").asString)
        assertEquals("1234567890", item.get("telefono").asString)
        assertEquals("admin_bot", item.get("creado_por").asString)
    }

    @Test
    fun testAddUserSuccessAndSavesRequiredFields() = runBlocking {
        val db = LocalDatabase.get(context)
        val payload = """
            {
              "usuario": "nuevo_cliente",
              "password": "Password123!",
              "vence": "2027-02-15",
              "modo": "registrar_existente",
              "adultos": true,
              "origen": "registro",
              "telefono": "+525511223344",
              "creado_por": "whatsapp_bot"
            }
        """.trimIndent()

        val envelope = createSignedEnvelope("ADD_USER", payload)
        val response = LinkCommandExecutor.handleMessage(context, envelope.toJson())

        assertNotNull(response)
        val respPayload = LinkResponsePayload.fromJson(response!!.payload)
        assertTrue(respPayload!!.ok)
        assertEquals(LinkCodes.OK, respPayload.code)

        val user = db.findUser("nuevo_cliente")
        assertNotNull(user)
        assertEquals("nuevo_cliente", user?.usuario)
        assertEquals("Password123!", user?.password)
        assertEquals("2027-02-15", user?.vencimiento)
        assertTrue(user!!.adultos)
        assertEquals("registro", user.origen)
        assertEquals("+525511223344", user.telefono)
        assertEquals("whatsapp_bot", user.creado_por)
        assertNotNull(user.creadoEn)
        assertTrue(user.creadoEn!! > 0L)
    }

    @Test
    fun testAddUserDuplicateRejected() = runBlocking {
        val db = LocalDatabase.get(context)
        db.saveUsers(listOf(UserModel(usuario = "cliente_existente", password = "pwd", vencimiento = "2027-02-15")))

        val payload = """{"usuario":"cliente_existente","password":"Password123!","vence":"2027-02-15","modo":"registrar_existente"}"""
        val envelope = createSignedEnvelope("ADD_USER", payload)
        val response = LinkCommandExecutor.handleMessage(context, envelope.toJson())

        assertNotNull(response)
        val respPayload = LinkResponsePayload.fromJson(response!!.payload)
        assertFalse(respPayload!!.ok)
        assertEquals(LinkCodes.DUPLICATE_USER, respPayload.code)
    }

    @Test
    fun testExtendUserCommand() = runBlocking {
        val db = LocalDatabase.get(context)
        db.saveUsers(listOf(UserModel(usuario = "extend_me", password = "pwd", vencimiento = "2026-11-01")))

        val payload = """{"usuario":"extend_me","base":"desde_hoy","dias":60}"""
        val envelope = createSignedEnvelope("EXTEND_USER", payload)
        val response = LinkCommandExecutor.handleMessage(context, envelope.toJson())

        assertNotNull(response)
        val respPayload = LinkResponsePayload.fromJson(response!!.payload)
        assertTrue(respPayload!!.ok)
        assertEquals(LinkCodes.OK, respPayload.code)

        val updated = db.findUser("extend_me")
        assertNotNull(updated)
        assertNotEquals("2026-11-01", updated?.vencimiento)
    }

    @Test
    fun testDeleteUserCommand() = runBlocking {
        val db = LocalDatabase.get(context)
        db.saveUsers(listOf(UserModel(usuario = "delete_target", password = "pwd", vencimiento = "2027-01-01")))

        // 1. Sin credenciales de panel configuradas, debe fallar con PANEL_LOGIN_FAILED y NO borrar en local
        val payload = """{"usuario":"delete_target"}"""
        val envelope = createSignedEnvelope("DELETE_USER", payload)
        val response = LinkCommandExecutor.handleMessage(context, envelope.toJson())

        assertNotNull(response)
        val respPayload = LinkResponsePayload.fromJson(response!!.payload)
        assertFalse(respPayload!!.ok)
        assertEquals(LinkCodes.PANEL_LOGIN_FAILED, respPayload.code)

        // El usuario DEBE seguir existiendo en la base local
        assertNotNull(db.findUser("delete_target"))

        // 2. Intentar borrar usuario que no existe devuelve NOT_FOUND
        val response2 = LinkCommandExecutor.handleMessage(context, createSignedEnvelope("DELETE_USER", """{"usuario":"no_existe"}""").toJson())
        val respPayload2 = LinkResponsePayload.fromJson(response2!!.payload)
        assertFalse(respPayload2!!.ok)
        assertEquals(LinkCodes.NOT_FOUND, respPayload2.code)
    }

    @Test
    fun testIdempotencyCachedResponse() = runBlocking {
        val envelope = createSignedEnvelope("PING", "{}", id = "fixed-uuid-12345")
        val response1 = LinkCommandExecutor.handleMessage(context, envelope.toJson())
        val response2 = LinkCommandExecutor.handleMessage(context, envelope.toJson())

        assertNotNull(response1)
        assertNotNull(response2)
        assertEquals(response1!!.id, response2!!.id)
        assertEquals(response1.payload, response2.payload)
    }

    @Test
    fun testCorruptDatabaseRejectsWriteWithInternal() = runBlocking {
        val file = File(context.filesDir, "sdmx_users.json")
        file.writeText("corrupt json")
        LocalDatabase.resetForTesting()
        val db = LocalDatabase.get(context)
        assertTrue(db.hasLoadError.value)

        val payload = """{"usuario":"some_user","password":"Password123!","vence":"2027-01-15","modo":"registrar_existente"}"""
        val envelope = createSignedEnvelope("ADD_USER", payload)
        val response = LinkCommandExecutor.handleMessage(context, envelope.toJson())

        assertNotNull(response)
        val respPayload = LinkResponsePayload.fromJson(response!!.payload)
        assertFalse(respPayload!!.ok)
        assertEquals(LinkCodes.INTERNAL, respPayload.code)
        assertTrue(respPayload.msg.contains("dañada", ignoreCase = true))
    }

    @Test
    fun testBadSignatureRejected() = runBlocking {
        val envelope = LinkEnvelope(
            v = 1,
            id = "bad-sig-id",
            ts = System.currentTimeMillis() / 1000L,
            cmd = "PING",
            payload = "{}",
            sig = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        )
        val response = LinkCommandExecutor.handleMessage(context, envelope.toJson())
        assertNotNull(response)
        val respPayload = LinkResponsePayload.fromJson(response!!.payload)
        assertFalse(respPayload!!.ok)
        assertEquals(LinkCodes.BAD_SIGNATURE, respPayload.code)
    }

    @Test
    fun testAddUserDuplicateIdenticalReturnsOkYaExistia() = runBlocking {
        val db = LocalDatabase.get(context)
        db.saveUsers(listOf(UserModel(id = "999", usuario = "cliente_identico", password = "Password123!", vencimiento = "2027-02-15", adultos = false)))

        val payload = """{"usuario":"cliente_identico","password":"Password123!","vence":"2027-02-15","adultos":false,"modo":"registrar_existente"}"""
        val envelope = createSignedEnvelope("ADD_USER", payload)
        val response = LinkCommandExecutor.handleMessage(context, envelope.toJson())

        assertNotNull(response)
        val respPayload = LinkResponsePayload.fromJson(response!!.payload)
        assertTrue(respPayload!!.ok)
        assertEquals(LinkCodes.OK, respPayload.code)

        val data = gson.toJsonTree(respPayload.data).asJsonObject
        assertTrue(data.get("ya_existia").asBoolean)
        assertEquals("cliente_identico", data.get("usuario").asString)
        assertEquals("999", data.get("id").asString)
    }

    @Test
    fun testAddUserQueuedWhenCycleRunning() = runBlocking {
        // Lock the execution mutex to simulate an ongoing renewal cycle
        com.example.worker.SdmxExecutionEngine.executionMutex.lock()
        try {
            val payload = """{"usuario":"cliente_durante_ciclo","password":"Password123!","vence":"2027-05-20","modo":"registrar_existente"}"""
            val envelope = createSignedEnvelope("ADD_USER", payload)
            val response = LinkCommandExecutor.handleMessage(context, envelope.toJson())

            assertNotNull(response)
            val respPayload = LinkResponsePayload.fromJson(response!!.payload)
            assertTrue(respPayload!!.ok)
            assertEquals(LinkCodes.QUEUED, respPayload.code)
            assertTrue(respPayload.msg.contains("Ciclo de renovación en curso", ignoreCase = true))
        } finally {
            com.example.worker.SdmxExecutionEngine.executionMutex.unlock()
        }
    }

    @Test
    fun testMaskPassword() {
        assertEquals("Ab******", LinkCommandExecutor.maskPassword("Ab123456"))
        assertEquals("**", LinkCommandExecutor.maskPassword("12"))
        assertEquals("", LinkCommandExecutor.maskPassword(""))
    }

    @Test
    fun testListUsersFilteringSortingAndOutOfBounds() = runBlocking {
        val db = LocalDatabase.get(context)
        db.saveUsers(listOf(
            UserModel(id = "1", usuario = "ana_client", password = "pwd", vencimiento = "2029-01-15", creado_por = "admin_bot"),
            UserModel(id = "2", usuario = "carlos_client", password = "pwd", vencimiento = "2027-05-10", creado_por = "whatsapp"),
            UserModel(id = "3", usuario = "beto_expired", password = "pwd", vencimiento = "2020-01-01", creado_por = "admin_bot")
        ))

        // 1. Filter by estado: "vigentes"
        val envVigentes = createSignedEnvelope("LIST_USERS", """{"pagina":1,"estado":"vigentes"}""")
        val respVigentes = LinkCommandExecutor.handleMessage(context, envVigentes.toJson())
        assertNotNull(respVigentes)
        val payloadVig = LinkResponsePayload.fromJson(respVigentes!!.payload)
        assertTrue(payloadVig!!.ok)
        val dataVig = gson.toJsonTree(payloadVig.data).asJsonObject
        assertEquals(2, dataVig.get("total").asInt)
        val itemsVig = dataVig.getAsJsonArray("items")
        // Sorted ascending: 2027-05-10 before 2029-01-15
        assertEquals("carlos_client", itemsVig.get(0).asJsonObject.get("usuario").asString)
        assertEquals("ana_client", itemsVig.get(1).asJsonObject.get("usuario").asString)

        // 2. Filter by creado_por: "whatsapp"
        val envCreadoPor = createSignedEnvelope("LIST_USERS", """{"pagina":1,"creado_por":"whatsapp"}""")
        val respCreadoPor = LinkCommandExecutor.handleMessage(context, envCreadoPor.toJson())
        val payloadCp = LinkResponsePayload.fromJson(respCreadoPor!!.payload)
        val dataCp = gson.toJsonTree(payloadCp!!.data).asJsonObject
        assertEquals(1, dataCp.get("total").asInt)
        assertEquals("carlos_client", dataCp.getAsJsonArray("items").get(0).asJsonObject.get("usuario").asString)

        // 3. Filter by buscar: "expired"
        val envBuscar = createSignedEnvelope("LIST_USERS", """{"pagina":1,"buscar":"expired"}""")
        val respBuscar = LinkCommandExecutor.handleMessage(context, envBuscar.toJson())
        val payloadBuscar = LinkResponsePayload.fromJson(respBuscar!!.payload)
        val dataBuscar = gson.toJsonTree(payloadBuscar!!.data).asJsonObject
        assertEquals(1, dataBuscar.get("total").asInt)
        assertEquals("beto_expired", dataBuscar.getAsJsonArray("items").get(0).asJsonObject.get("usuario").asString)

        // 4. Out of bounds page returns BAD_REQUEST
        val envOutOfRange = createSignedEnvelope("LIST_USERS", """{"pagina":99}""")
        val respOutOfRange = LinkCommandExecutor.handleMessage(context, envOutOfRange.toJson())
        val payloadOor = LinkResponsePayload.fromJson(respOutOfRange!!.payload)
        assertFalse(payloadOor!!.ok)
        assertEquals(LinkCodes.BAD_REQUEST, payloadOor.code)
    }

    @Test
    fun testExtendUserValidationsAndFromVencimiento() = runBlocking {
        val db = LocalDatabase.get(context)
        db.saveUsers(listOf(
            UserModel(usuario = "valid_user", password = "pwd", vencimiento = "2027-01-01")
        ))

        // Missing usuario -> BAD_REQUEST
        val respNoUser = LinkCommandExecutor.handleMessage(context, createSignedEnvelope("EXTEND_USER", """{"dias":10}""").toJson())
        val payloadNoUser = LinkResponsePayload.fromJson(respNoUser!!.payload)
        assertFalse(payloadNoUser!!.ok)
        assertEquals(LinkCodes.BAD_REQUEST, payloadNoUser.code)

        // Missing dias and meses -> BAD_REQUEST
        val respNoDuration = LinkCommandExecutor.handleMessage(context, createSignedEnvelope("EXTEND_USER", """{"usuario":"valid_user","dias":0,"meses":0}""").toJson())
        val payloadNoDuration = LinkResponsePayload.fromJson(respNoDuration!!.payload)
        assertFalse(payloadNoDuration!!.ok)
        assertEquals(LinkCodes.BAD_REQUEST, payloadNoDuration.code)

        // User not found -> NOT_FOUND
        val respNotFound = LinkCommandExecutor.handleMessage(context, createSignedEnvelope("EXTEND_USER", """{"usuario":"not_existing","dias":10}""").toJson())
        val payloadNotFound = LinkResponsePayload.fromJson(respNotFound!!.payload)
        assertFalse(payloadNotFound!!.ok)
        assertEquals(LinkCodes.NOT_FOUND, payloadNotFound.code)

        // Extend 1 month from vencimiento
        val respExtend = LinkCommandExecutor.handleMessage(context, createSignedEnvelope("EXTEND_USER", """{"usuario":"valid_user","base":"desde_vencimiento","meses":1}""").toJson())
        val payloadExtend = LinkResponsePayload.fromJson(respExtend!!.payload)
        assertTrue(payloadExtend!!.ok)
        val dataExtend = gson.toJsonTree(payloadExtend.data).asJsonObject
        assertEquals("2027-01-01", dataExtend.get("vence_anterior").asString)
        assertEquals("2027-02-01", dataExtend.get("vence_nueva").asString)

        val updatedUser = db.findUser("valid_user")
        assertEquals("2027-02-01", updatedUser?.vencimiento)
    }
}
