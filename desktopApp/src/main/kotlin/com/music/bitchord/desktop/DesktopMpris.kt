package com.music.bitchord.desktop

import com.music.bitchord.data.model.Song
import com.music.bitchord.data.model.durationMillis
import org.freedesktop.dbus.DBusPath
import org.freedesktop.dbus.TypeRef
import org.freedesktop.dbus.annotations.DBusInterfaceName
import org.freedesktop.dbus.annotations.DBusProperty
import org.freedesktop.dbus.annotations.DBusProperty.Access
import org.freedesktop.dbus.connections.impl.DBusConnection
import org.freedesktop.dbus.connections.impl.DBusConnectionBuilder
import org.freedesktop.dbus.exceptions.DBusException
import org.freedesktop.dbus.exceptions.DBusExecutionException
import org.freedesktop.dbus.interfaces.DBusInterface
import org.freedesktop.dbus.interfaces.Properties
import org.freedesktop.dbus.messages.DBusSignal
import org.freedesktop.dbus.types.Variant
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.math.abs

private const val MPRIS_ROOT_INTERFACE = "org.mpris.MediaPlayer2"
private const val MPRIS_PLAYER_INTERFACE = "org.mpris.MediaPlayer2.Player"
private const val MPRIS_OBJECT_PATH = "/org/mpris/MediaPlayer2"
private const val MPRIS_BUS_NAME = "org.mpris.MediaPlayer2.bitchord"

/** A position change bigger than this, beyond what normal playback explains, is announced as a seek. */
private const val SEEK_JUMP_MS = 2_500L

interface MprisMetadataType : TypeRef<Map<String, Variant<*>>>

@DBusInterfaceName(MPRIS_ROOT_INTERFACE)
@DBusProperty(name = "CanQuit", type = Boolean::class, access = Access.READ)
@DBusProperty(name = "CanRaise", type = Boolean::class, access = Access.READ)
@DBusProperty(name = "HasTrackList", type = Boolean::class, access = Access.READ)
@DBusProperty(name = "Identity", type = String::class, access = Access.READ)
@DBusProperty(name = "DesktopEntry", type = String::class, access = Access.READ)
@DBusProperty(name = "SupportedUriSchemes", type = MprisUriSchemesType::class, access = Access.READ)
@DBusProperty(name = "SupportedMimeTypes", type = MprisMimeTypesType::class, access = Access.READ)
interface MprisRoot : DBusInterface {
    fun Raise()
    fun Quit()
}

interface MprisUriSchemesType : TypeRef<List<String>>

interface MprisMimeTypesType : TypeRef<List<String>>

@DBusInterfaceName(MPRIS_PLAYER_INTERFACE)
@DBusProperty(name = "Metadata", type = MprisMetadataType::class, access = Access.READ)
@DBusProperty(name = "PlaybackStatus", type = String::class, access = Access.READ)
@DBusProperty(name = "LoopStatus", type = String::class, access = Access.READ_WRITE)
@DBusProperty(name = "Rate", type = Double::class, access = Access.READ_WRITE)
@DBusProperty(name = "Shuffle", type = Boolean::class, access = Access.READ_WRITE)
@DBusProperty(name = "Volume", type = Double::class, access = Access.READ_WRITE)
@DBusProperty(name = "Position", type = Long::class, access = Access.READ)
@DBusProperty(name = "MinimumRate", type = Double::class, access = Access.READ)
@DBusProperty(name = "MaximumRate", type = Double::class, access = Access.READ)
@DBusProperty(name = "CanGoNext", type = Boolean::class, access = Access.READ)
@DBusProperty(name = "CanGoPrevious", type = Boolean::class, access = Access.READ)
@DBusProperty(name = "CanPlay", type = Boolean::class, access = Access.READ)
@DBusProperty(name = "CanPause", type = Boolean::class, access = Access.READ)
@DBusProperty(name = "CanSeek", type = Boolean::class, access = Access.READ)
@DBusProperty(name = "CanControl", type = Boolean::class, access = Access.READ)
interface MprisPlayer : DBusInterface {
    fun Previous()
    fun Next()
    fun Stop()
    fun Play()
    fun Pause()
    fun PlayPause()
    fun Seek(offsetUs: Long)
    fun SetPosition(trackId: DBusPath, positionUs: Long)
    fun OpenUri(uri: String)

    /** Emitted when the position jumps; clients resync their progress bar from it. */
    class Seeked @Throws(DBusException::class) constructor(path: String, val position: Long) :
        DBusSignal(path, position)
}

/** Publishes the desktop player's state through the standard Linux MPRIS session-bus interfaces. */
internal class DesktopMprisController(
    private val onPlay: () -> Unit,
    private val onPause: () -> Unit,
    private val onPlayPause: () -> Unit,
    private val onNext: () -> Unit,
    private val onPrevious: () -> Unit,
    private val onShuffleChanged: (Boolean) -> Unit,
    private val onLoopStatusChanged: (String) -> Unit,
    private val onRateChanged: (Double) -> Unit,
    private val onVolumeChanged: (Double) -> Unit,
    /** Absolute position in ms. Returns false when the seek was refused (e.g. controls locked). */
    private val onSeek: (Long) -> Boolean,
) {
    private val lock = Any()
    private val worker: ExecutorService = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "BitChord-MPRIS").apply { isDaemon = true }
    }
    private var connection: DBusConnection? = null
    private var started = false
    private var state = DesktopPlaybackState()
    private var shuffle = false
    private var loopStatus = "None"
    private var rate = 1.0

    // What clients were last told, so only real changes are published.
    private var publishedSongId: String? = null
    private var publishedDurationMs = 0L
    private var publishedPlaybackStatus: String? = null
    private var publishedShuffle = false
    private var publishedLoopStatus = "None"
    private var publishedRate = 1.0
    private var publishedVolume = 1.0
    private var publishedCaps: Map<String, Boolean> = emptyMap()

    // Anchor for extrapolating Position between updates.
    private var lastPositionMs = 0L
    private var lastUpdateNanos = System.nanoTime()

    fun start() {
        if (!isLinux() || synchronized(lock) { started }) return
        synchronized(lock) { started = true }
        worker.execute {
            runCatching {
                // Not shared.
                val bus = DBusConnectionBuilder.forSessionBus().withShared(false).build()
                bus.requestBusName(MPRIS_BUS_NAME)
                bus.exportObject(MPRIS_OBJECT_PATH, ExportedPlayer(this@DesktopMprisController))
                synchronized(lock) { connection = bus }
                publish(force = true)
            }.onFailure { failure ->
                synchronized(lock) {
                    connection?.close()
                    connection = null
                    started = false
                }
                System.err.println("BitChord MPRIS unavailable: ${failure.message}")
            }
        }
    }

    fun stop() {
        val bus = synchronized(lock) {
            started = false
            val current = connection
            connection = null
            current
        }
        runCatching { bus?.close() }
        worker.shutdownNow()
    }

    fun update(
        playback: DesktopPlaybackState,
        shuffle: Boolean,
        loopStatus: String,
        rate: Double,
    ) {
        var seekedToMs: Long? = null
        synchronized(lock) {
            val now = System.nanoTime()
            val elapsedMs =
                if (state.isPlaying) ((now - lastUpdateNanos) / 1_000_000.0 * this.rate).toLong() else 0L
            val expected = lastPositionMs + elapsedMs
            val newSong = state.song?.videoId != playback.song?.videoId
            if (playback.song != null && (newSong || abs(playback.positionMs - expected) > SEEK_JUMP_MS)) {
                seekedToMs = playback.positionMs.coerceAtLeast(0L)
            }
            lastPositionMs = playback.positionMs
            lastUpdateNanos = now

            state = playback
            this.shuffle = shuffle
            this.loopStatus = loopStatus
            this.rate = rate.coerceIn(0.25, 3.0)
        }
        publish(force = false) // Metadata first, so clients know the new track...
        seekedToMs?.let(::emitSeeked) // ...then where in it we are
    }

    /** Relative seek from a client: [deltaMs] is added to the current position. */
    fun seekBy(deltaMs: Long) {
        val (target, duration) = synchronized(lock) {
            (currentPositionMs() + deltaMs) to durationMs(state.song, state.durationMs)
        }
        when {
            target < 0L -> performSeek(0L)
            duration > 0L && target > duration -> onNext() // the spec says seeking past the end skips
            else -> performSeek(target)
        }
    }

    /** Absolute seek from a client, for a specific track. */
    fun setPosition(trackId: String, positionMs: Long) {
        val (song, duration) = synchronized(lock) { state.song to durationMs(state.song, state.durationMs) }
        if (song == null || trackPath(song) != trackId) return // stale request for a previous track
        if (positionMs < 0L || (duration > 0L && positionMs > duration)) return
        performSeek(positionMs)
    }

    /** Call whenever the position is moved by anything other than normal playback. */
    fun notifySeeked(positionMs: Long) {
        val position = positionMs.coerceAtLeast(0L)
        synchronized(lock) {
            // Re-anchor the extrapolation, so Position polls are right immediately.
            lastPositionMs = position
            lastUpdateNanos = System.nanoTime()
            state = state.copy(positionMs = position) // assumes DesktopPlaybackState is a data class
        }
        // Keep D-Bus I/O off the caller's thread; the worker is gone after stop().
        runCatching { worker.execute { emitSeeked(position) } }
    }

    private fun performSeek(positionMs: Long) {
        if (onSeek(positionMs)) notifySeeked(positionMs)
    }

    private fun emitSeeked(positionMs: Long) {
        val bus = synchronized(lock) { connection } ?: return
        runCatching {
            bus.sendMessage(MprisPlayer.Seeked(MPRIS_OBJECT_PATH, positionMs * 1_000L))
        }.onFailure { System.err.println("BitChord MPRIS: Seeked failed: ${it.message}") }
    }

    /** Call with [lock] held. */
    private fun currentPositionMs(): Long {
        val base = state.positionMs.coerceAtLeast(0L)
        val position = if (state.isPlaying) {
            base + ((System.nanoTime() - lastUpdateNanos) / 1_000_000.0 * rate).toLong()
        } else {
            base
        }
        val duration = durationMs(state.song, state.durationMs)
        return if (duration > 0L) position.coerceAtMost(duration) else position
    }

    private fun capabilities(playback: DesktopPlaybackState): Map<String, Boolean> {
        val hasSong = playback.song != null
        return linkedMapOf(
            "CanGoNext" to hasSong,
            "CanGoPrevious" to hasSong,
            "CanPlay" to (hasSong && playback.error == null),
            "CanPause" to hasSong,
            "CanSeek" to (hasSong && durationMs(playback.song, playback.durationMs) > 0L),
            "CanControl" to true,
        )
    }

    private fun publish(force: Boolean) {
        val currentConnection: DBusConnection
        val changed: MutableMap<String, Variant<*>>
        synchronized(lock) {
            currentConnection = connection ?: return
            val playbackStatus = playbackStatus(state)
            val songId = state.song?.videoId
            val duration = durationMs(state.song, state.durationMs)
            changed = linkedMapOf()
            // Duration is part of Metadata (mpris:length) and often arrives after the song starts.
            if (force || publishedSongId != songId || publishedDurationMs != duration) {
                changed["Metadata"] = variant(metadata(state.song), "a{sv}")
            }
            if (force || publishedPlaybackStatus != playbackStatus) {
                changed["PlaybackStatus"] = variant(playbackStatus)
            }
            if (force || publishedShuffle != shuffle) {
                changed["Shuffle"] = variant(shuffle)
            }
            if (force || publishedLoopStatus != loopStatus) {
                changed["LoopStatus"] = variant(loopStatus)
            }
            if (force || publishedRate != rate) {
                changed["Rate"] = variant(rate)
            }
            if (force || publishedVolume != state.volume.toDouble()) {
                changed["Volume"] = variant(state.volume.toDouble().coerceIn(0.0, 1.0))
            }
            val caps = capabilities(state)
            caps.forEach { (name, value) ->
                if (force || publishedCaps[name] != value) changed[name] = variant(value)
            }
            publishedCaps = caps
            publishedSongId = songId
            publishedDurationMs = duration
            publishedPlaybackStatus = playbackStatus
            publishedShuffle = shuffle
            publishedLoopStatus = loopStatus
            publishedRate = rate
            publishedVolume = state.volume.toDouble()
        }
        if (changed.isEmpty()) return
        runCatching {
            currentConnection.sendMessage(
                Properties.PropertiesChanged(
                    MPRIS_OBJECT_PATH,
                    MPRIS_PLAYER_INTERFACE,
                    changed,
                    emptyList(),
                ),
            )
        }
    }

    /** One property. Position is answered directly, since clients poll it often. */
    private fun property(interfaceName: String, propertyName: String): Variant<*> {
        if (interfaceName == MPRIS_PLAYER_INTERFACE && propertyName == "Position") {
            return variant(synchronized(lock) { currentPositionMs() } * 1_000L)
        }
        return allProperties(interfaceName)[propertyName] ?: unknownProperty(interfaceName, propertyName)
    }

    private fun allProperties(interfaceName: String): Map<String, Variant<*>> = when (interfaceName) {
        MPRIS_ROOT_INTERFACE -> linkedMapOf(
            "CanQuit" to variant(false),
            "CanRaise" to variant(false),
            "HasTrackList" to variant(false),
            "Identity" to variant("BitChord"),
            "DesktopEntry" to variant("bitchord"),
            "SupportedUriSchemes" to variant(listOf("file", "http", "https"), "as"),
            "SupportedMimeTypes" to variant(
                listOf("audio/mpeg", "audio/mp4", "audio/aac", "audio/flac", "audio/ogg"),
                "as",
            ),
        )
        MPRIS_PLAYER_INTERFACE -> synchronized(lock) {
            linkedMapOf<String, Variant<*>>(
                "Metadata" to variant(metadata(state.song), "a{sv}"),
                "PlaybackStatus" to variant(playbackStatus(state)),
                "LoopStatus" to variant(loopStatus),
                "Rate" to variant(rate),
                "Shuffle" to variant(shuffle),
                "Volume" to variant(state.volume.toDouble().coerceIn(0.0, 1.0)),
                "Position" to variant(currentPositionMs() * 1_000L),
                "MinimumRate" to variant(0.25),
                "MaximumRate" to variant(3.0),
            ).also { map ->
                // Same source as publish(), so the two cannot drift apart.
                capabilities(state).forEach { (name, value) -> map[name] = variant(value) }
            }
        }
        else -> unknownProperty(interfaceName, "")
    }

    private fun setProperty(interfaceName: String, propertyName: String, value: Any?) {
        val unwrapped = (value as? Variant<*>)?.value ?: value
        if (interfaceName != MPRIS_PLAYER_INTERFACE) throw unknownProperty(interfaceName, propertyName)
        when (propertyName) {
            "Shuffle" -> (unwrapped as? Boolean)?.let(onShuffleChanged)
                ?: throw DBusExecutionException("Shuffle expects a boolean")
            "LoopStatus" -> (unwrapped as? String)?.let(onLoopStatusChanged)
                ?: throw DBusExecutionException("LoopStatus expects a string")
            "Rate" -> (unwrapped as? Number)?.toDouble()?.let(onRateChanged)
                ?: throw DBusExecutionException("Rate expects a number")
            "Volume" -> (unwrapped as? Number)?.toDouble()?.let(onVolumeChanged)
                ?: throw DBusExecutionException("Volume expects a number")
            else -> throw unknownProperty(interfaceName, propertyName)
        }
    }

    private fun metadata(song: Song?): Map<String, Variant<*>> = song?.let { current ->
        // A LinkedHashMap, deliberately: `buildMap` hands back an internal builder class that
        // dbus-java refuses to marshal.
        LinkedHashMap<String, Variant<*>>().apply {
            put("mpris:trackid", variant(DBusPath(trackPath(current))))
            put("xesam:title", variant(current.title))
            if (current.artist.isNotBlank()) put("xesam:artist", variant(listOf(current.artist), "as"))
            current.albumName?.takeIf(String::isNotBlank)?.let { put("xesam:album", variant(it)) }
            durationMs(current, state.durationMs).takeIf { it > 0L }?.let {
                put("mpris:length", variant(it * 1_000L))
            }
            current.thumbnailUrl?.takeIf(String::isNotBlank)?.let { put("mpris:artUrl", variant(it)) }
            current.localUri?.takeIf(String::isNotBlank)?.let { put("xesam:url", variant(it)) }
        }
    } ?: emptyMap()

    private fun unknownProperty(interfaceName: String, propertyName: String): Nothing =
        throw DBusExecutionException("Unknown property $interfaceName.$propertyName")

    private fun playbackStatus(playback: DesktopPlaybackState): String = when {
        playback.isPlaying -> "Playing"
        playback.song != null && playback.error == null -> "Paused"
        else -> "Stopped"
    }

    private fun durationMs(song: Song?, knownDurationMs: Long): Long =
        knownDurationMs.takeIf { it > 0L } ?: song?.durationText.durationMillis()

    private fun trackPath(song: Song): String =
        "$MPRIS_OBJECT_PATH/track/${Integer.toUnsignedString(song.videoId.hashCode(), 16)}"

    private fun variant(value: Any, signature: String? = null): Variant<*> =
        if (signature == null) Variant(value) else Variant(value, signature)

    private fun isLinux(): Boolean =
        DesktopPlatform.isLinux

    private class ExportedPlayer(private val controller: DesktopMprisController) :
        MprisRoot,
        MprisPlayer,
        Properties {
        override fun getObjectPath(): String = MPRIS_OBJECT_PATH

        override fun Raise() = Unit

        override fun Quit() = Unit

        override fun Previous() = controller.onPrevious()

        override fun Next() = controller.onNext()

        override fun Stop() = controller.onPause()

        override fun Play() = controller.onPlay()

        override fun Pause() = controller.onPause()

        override fun PlayPause() = controller.onPlayPause()

        // Seek is relative, SetPosition is absolute and names the track it was meant for.
        override fun Seek(offsetUs: Long) = controller.seekBy(offsetUs / 1_000L)

        override fun SetPosition(trackId: DBusPath, positionUs: Long) =
            controller.setPosition(trackId.path, positionUs / 1_000L)

        override fun OpenUri(uri: String) = Unit

        override fun <A> Get(interfaceName: String, propertyName: String): A =
            controller.property(interfaceName, propertyName) as A

        override fun <A> Set(interfaceName: String, propertyName: String, value: A) {
            controller.setProperty(interfaceName, propertyName, value)
        }

        override fun GetAll(interfaceName: String): Map<String, Variant<*>> =
            controller.allProperties(interfaceName)
    }
}
