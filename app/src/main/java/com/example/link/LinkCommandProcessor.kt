package com.example.link

import android.content.Context

/**
 * Fachada para el procesamiento de comandos del enlace bot según el CONTRATO v1.
 * Delegado a LinkCommandExecutor.
 */
object LinkCommandProcessor {
    suspend fun process(context: Context, envelope: LinkEnvelope): LinkEnvelope? {
        return LinkCommandExecutor.processEnvelope(context, envelope)
    }

    suspend fun processRaw(context: Context, rawJson: String): LinkEnvelope? {
        return LinkCommandExecutor.handleMessage(context, rawJson)
    }
}
