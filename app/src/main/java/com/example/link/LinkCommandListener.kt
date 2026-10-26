package com.example.link

import android.content.Context
import android.util.Log
import com.example.data.LogManager
import com.example.data.PreferencesManager
import kotlinx.coroutines.*
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.random.Random

class LinkCommandListener(private val context: Context) {

    companion object {
        private const val TAG = "LinkCommandListener"
        private val isRunning = AtomicBoolean(false)
        @Volatile private var activeInstance: LinkCommandListener? = null

        fun getActiveInstance(): LinkCommandListener? = activeInstance
    }

    private var listenerJob: Job? = null
    private var streamCall: okhttp3.Call? = null

    // Cliente OkHttp dedicado para streaming infinito con pingInterval de 30s
    private val client = OkHttpClient.Builder()
        .connectTimeout(30, TimeUnit.SECONDS)
        .readTimeout(0, TimeUnit.SECONDS)
        .pingInterval(30, TimeUnit.SECONDS)
        .retryOnConnectionFailure(true)
        .build()

    fun start(scope: CoroutineScope) {
        if (!isRunning.compareAndSet(false, true)) {
            Log.d(TAG, "Listener already running; skipping start.")
            return
        }
        activeInstance = this
        listenerJob = scope.launch(Dispatchers.IO) {
            runListenerLoop()
        }
    }

    fun stop() {
        isRunning.set(false)
        activeInstance = null
        try {
            streamCall?.cancel()
        } catch (_: Exception) {}
        listenerJob?.cancel()
        listenerJob = null
        PreferencesManager.setLinkStatus(context, "🔴 Apagado")
    }

    fun restart(scope: CoroutineScope) {
        stop()
        start(scope)
    }

    private suspend fun runListenerLoop() {
        var backoffMs = 2000L

        while (currentCoroutineContext().isActive && isRunning.get()) {
            val enabled = PreferencesManager.isLinkEnabled(context)
            if (!enabled) {
                PreferencesManager.setLinkStatus(context, "🔴 Apagado")
                delay(2000L)
                continue
            }

            val baseUrl = PreferencesManager.getNtfyBaseUrl(context).ifEmpty { PreferencesManager.DEFAULT_NTFY_BASE_URL }
            val cmdTopic = PreferencesManager.getLinkTopicCmd(context).ifEmpty { PreferencesManager.DEFAULT_LINK_TOPIC_CMD }
            val token = PreferencesManager.getLinkToken(context)

            val lastId = PreferencesManager.getLastNtfyId(context)
            val sinceParam = if (lastId.isNotBlank()) lastId else "all"

            val streamUrl = "${baseUrl.trimEnd('/')}/$cmdTopic/json?since=$sinceParam"
            LogManager.addLog(context, "📡 [Enlace Bot] Conectando streaming NTFY en '$cmdTopic' (since=$sinceParam)...")
            PreferencesManager.setLinkStatus(context, "🟡 Reconectando...")

            val connectStartTime = System.currentTimeMillis()

            try {
                val reqBuilder = Request.Builder()
                    .url(streamUrl)
                    .get()
                    .addHeader("Accept", "application/x-ndjson, application/json")

                if (token.isNotBlank()) {
                    reqBuilder.addHeader("Authorization", "Bearer $token")
                }

                val call = client.newCall(reqBuilder.build())
                streamCall = call

                call.execute().use { response ->
                    if (!response.isSuccessful) {
                        LogManager.addLog(context, "⚠️ [Enlace Bot] Error HTTP ${response.code} en streaming NTFY. Reconectando en ${backoffMs / 1000}s...")
                        PreferencesManager.setLinkStatus(context, "🟡 Reconectando...")
                        delay(backoffMs)
                        backoffMs = minOf(60_000L, backoffMs * 2) + Random.nextLong(500, 2000)
                        return@use
                    }

                    PreferencesManager.setLinkStatus(context, "🟢 Conectado")
                    LogManager.addLog(context, "🟢 [Enlace Bot] Escucha streaming activa en '$cmdTopic'")

                    val inputStream = response.body?.byteStream() ?: return@use
                    val reader = BufferedReader(InputStreamReader(inputStream, Charsets.UTF_8))

                    while (currentCoroutineContext().isActive && isRunning.get()) {
                        val line = reader.readLine() ?: break
                        if (line.isBlank()) continue

                        // Si la conexión ha sido estable más de 60s, reiniciar backoff a 2s
                        if (System.currentTimeMillis() - connectStartTime > 60_000L) {
                            backoffMs = 2000L
                        }

                        // Verificar tamaño máximo de mensaje (4096 bytes)
                        if (line.toByteArray(Charsets.UTF_8).size > 4096) {
                            LogManager.addLog(context, "⚠️ [Enlace Bot] Mensaje descartado: supera el límite de 4096 bytes.")
                            continue
                        }

                        try {
                            val json = JSONObject(line)
                            val event = json.optString("event")

                            // Ignorar eventos open y keepalive
                            if (event == "open" || event == "keepalive") {
                                continue
                            }

                            if (event == "message") {
                                val ntfyId = json.optString("id")
                                val messageBody = json.optString("message")

                                if (messageBody.isNotEmpty()) {
                                    // Procesar cada comando en su propia coroutine
                                    CoroutineScope(Dispatchers.IO).launch {
                                        try {
                                            LinkCommandProcessor.processRaw(context, messageBody)
                                            // Guardar id de ntfy y registrar estadística solo tras procesar
                                            if (ntfyId.isNotEmpty()) {
                                                PreferencesManager.setLastNtfyId(context, ntfyId)
                                            }
                                            PreferencesManager.recordCommandProcessed(context)
                                        } catch (e: Exception) {
                                            Log.e(TAG, "Error en coroutine procesando comando: ${e.message}", e)
                                        }
                                    }
                                }
                            }
                        } catch (e: Exception) {
                            LogManager.addLog(context, "⚠️ [Enlace Bot] Línea descartada por formato inválido: ${e.message}")
                        }
                    }
                }
            } catch (e: CancellationException) {
                break
            } catch (e: Exception) {
                PreferencesManager.setLinkStatus(context, "🟡 Reconectando...")
                Log.e(TAG, "Excepción en conexión NTFY: ${e.message}", e)
                LogManager.addLog(context, "⚠️ [Enlace Bot] Conexión interrumpida: ${e.message}. Reconectando en ${backoffMs / 1000}s...")
                delay(backoffMs)
                backoffMs = minOf(60_000L, backoffMs * 2) + Random.nextLong(500, 2000)
            } finally {
                streamCall = null
            }
        }

        PreferencesManager.setLinkStatus(context, "🔴 Apagado")
    }
}
