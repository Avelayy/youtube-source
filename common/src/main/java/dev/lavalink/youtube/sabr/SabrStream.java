package dev.lavalink.youtube.sabr;

import com.sedmelluq.discord.lavaplayer.tools.io.HttpClientTools;
import com.sedmelluq.discord.lavaplayer.tools.io.HttpInterface;
import com.sedmelluq.discord.lavaplayer.tools.io.SeekableInputStream;
import com.sedmelluq.discord.lavaplayer.track.info.AudioTrackInfoProvider;
import dev.lavalink.youtube.sabr.protobuf.ProtoReader;
import dev.lavalink.youtube.sabr.protobuf.ProtoWriter;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * A {@link SeekableInputStream} that plays a YouTube SABR (Server Adaptive Bitrate) audio stream.
 *
 * <p>SABR formats do not expose a plain progressive download URL. Instead, media is retrieved by
 * POSTing a protobuf {@code VideoPlaybackAbrRequest} to a server ABR streaming URL and parsing the
 * UMP-formatted response into media segments. This class runs that request loop lazily as bytes are
 * consumed, concatenating the initialization segment followed by each media segment in order to
 * reconstruct a continuous (fragmented MP4 or WebM) byte stream that Lavaplayer's container readers
 * can consume.</p>
 *
 * <p>The reconstructed bytes are kept in memory so that nearby (including small backwards) seeks are
 * satisfied without re-downloading. Memory is bounded: the initialization segment is always kept,
 * but media further than {@link #MAX_RETAINED_BEHIND_BYTES} behind the read position is evicted.
 * A seek into evicted media simply re-aims the SABR timeline.</p>
 */
public class SabrStream extends SeekableInputStream {
    private static final Logger log = LoggerFactory.getLogger(SabrStream.class);

    private static final long MAX_SKIP_DISTANCE = 512L * 1024L;
    // Small forward jumps are cheaper and safer to satisfy from the normal sequential SABR flow.
    private static final long FAST_SEEK_MIN_GAP = 2L * 1024L * 1024L;
    // Keep some media after the requested point available so container readers can inspect
    // clusters/cues without immediately hitting the logical end of the SABR session.
    private static final long FAST_SEEK_END_MARGIN_MS = 30_000L;
    // Fetch SABR media before the exact requested time. Matroska seeks to the cue/cluster before
    // the requested timestamp and then discards frames until minimumTimecode. Fetching exactly at
    // the user timestamp can therefore place a later cluster at an earlier cue byte offset.
    private static final long EXACT_SEEK_PREROLL_MS = 0L;
    private static final long SEEK_PREFETCH_BYTES = 1024L * 1024L;
    // A cached seek is safe only when the target byte belongs to a genuinely contiguous
    // neighbourhood. A single cached byte is not enough for Matroska to continue parsing.
    private static final long LOCAL_SEEK_MIN_CONTIGUOUS_BYTES = 512L * 1024L;
    // Require some valid bytes immediately before the cue as well. Matroska may walk backwards
    // around cue/cluster boundaries while resolving a time seek.
    private static final long LOCAL_SEEK_MIN_BACKTRACK_BYTES = 128L * 1024L;
    private static final int MAX_REQUEST_RETRIES = 3;
    private static final int ENABLED_TRACK_TYPES_AUDIO_ONLY = 1;
    // A hard cap to guard against pathological request loops that never make progress.
    private static final int MAX_REQUESTS = 10000;
    // Already-played media further than this behind readPos is evicted (init segment is kept), so
    // memory stays bounded for hour-long tracks instead of growing with playback time.
    private static final long MAX_RETAINED_BEHIND_BYTES = 4L * 1024L * 1024L;
    // Pause between retries of a failed request, multiplied by the attempt number.
    private static final long RETRY_DELAY_MS = 200L;
    // A response without media is not necessarily the end of the stream (the server may only send
    // a policy asking us to back off), so allow a few of them before giving up.
    private static final int MAX_CONSECUTIVE_EMPTY_RESPONSES = 3;
    private static final long EMPTY_RESPONSE_DELAY_MS = 250L;
    private static final long MAX_BACKOFF_MS = 10_000L;

    private final HttpInterface httpInterface;
    private final byte[] ustreamerConfig;
    private final byte[] poToken;
    private final SabrClientInfo clientInfo;
    private final FormatId audioFormatId;
    private final boolean drcEnabled;
    private final long contentLength;

    private final ChunkedByteBuffer buffer = new ChunkedByteBuffer();

    private URI serverAbrStreamingUrl;
    private long readPos;
    private boolean finished;
    private int requestNumber;
    private int requestCount;

    // Bookkeeping for the init segment, eviction and empty-response handling.
    private long initSegmentLength;
    private long evictedBefore;
    private long storedSegmentCount;
    private int consecutiveEmptyResponses;
    private long policyBackoffMs;

    private long durationMs;
    private long downloadedDurationMs;

    // Fast-seek state. SABR requests are time based while Lavaplayer seeks by byte offset.
    // For a forward hard seek, estimate the target playback time from the known file length and
    // place the returned media at the byte offset requested by the container reader.
    private long writePos;
    // Byte offset where the most recent fast seek (re-aim) placed its media, or -1.
    private long lastFastSeekStart = -1L;
    private boolean fastSeekActive;
    private long pendingExactSeekTimeMs = -1L;

    // Per-stream SABR state.
    private boolean formatInitialized;
    private final List<MediaHeader> lastMediaHeaders = new ArrayList<>();
    private final Set<Integer> downloadedSegments = new HashSet<>();
    private List<BufferedRangeData> cachedBufferedRanges;
    private byte[] playbackCookie;
    private int streamProtectionStatus;

    // SABR context state (mainly for ad handling).
    private final Map<Integer, byte[]> sabrContextValues = new HashMap<>();
    private final Set<Integer> activeSabrContextTypes = new HashSet<>();

    // Transient per-request state.
    private final Map<Integer, PartialSegment> partialSegmentQueue = new HashMap<>();

    public SabrStream(@NotNull HttpInterface httpInterface,
                      @NotNull URI serverAbrStreamingUrl,
                      @NotNull byte[] ustreamerConfig,
                      @Nullable byte[] poToken,
                      @NotNull SabrClientInfo clientInfo,
                      @NotNull FormatId audioFormatId,
                      boolean drcEnabled,
                      long contentLength,
                      long durationMs) {
        super(contentLength, MAX_SKIP_DISTANCE);
        this.httpInterface = httpInterface;
        this.serverAbrStreamingUrl = serverAbrStreamingUrl;
        this.ustreamerConfig = ustreamerConfig;
        this.poToken = poToken;
        this.clientInfo = clientInfo;
        this.audioFormatId = audioFormatId;
        this.drcEnabled = drcEnabled;
        this.contentLength = contentLength;
        this.durationMs = durationMs > 0 ? durationMs : Long.MAX_VALUE;
    }

    //<editor-fold desc="SeekableInputStream API">
    @Override
    public long getPosition() {
        return readPos;
    }

    /**
     * Supplies the exact user-requested playback time for the next hard seek performed by the
     * container parser. This avoids estimating a timestamp from a WebM/MP4 byte offset.
     */
    public void prepareTimeSeek(long timeMs) {
        long upperBound = durationMs == Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(0L, durationMs - 1L);
        pendingExactSeekTimeMs = Math.min(Math.max(0L, timeMs), upperBound);
        log.debug("SABR exact time seek prepared: {}ms", pendingExactSeekTimeMs);
    }

    /**
     * Ends the short exact-seek scope opened by prepareTimeSeek(). Some container seeks are
     * satisfied entirely from their own indexes and never call seekHard(). V7 left the timestamp
     * armed in that case, so a later parser/internal hard seek could accidentally re-aim SABR to
     * an old user position.
     */
    public void finishTimeSeek() {
        if (pendingExactSeekTimeMs >= 0) {
            log.debug("SABR exact time seek completed without hard seek: {}ms", pendingExactSeekTimeMs);
            pendingExactSeekTimeMs = -1L;
        }
    }

    @Override
    protected void seekHard(long position) throws IOException {
        if (position < 0) {
            position = 0;
        }

        long exactTimeMs = pendingExactSeekTimeMs;
        long cachedAhead = buffer.contiguousAvailable(position);
        long cachedBehind = buffer.contiguousBefore(position);

        // V7 hybrid seek: an explicit time seek may stay completely local, but only when the
        // requested Matroska byte offset is inside a sufficiently large *contiguous* cached
        // neighbourhood. This is deliberately stronger than hasByte(position): V5 showed that a
        // sparse single-byte/cache hit can strand MatroskaFileReader.skip() at the next hole.
        //
        // One extra case is safe: seeking to the very byte where the last re-aim started. The parser
        // already consumed exactly that entry point (nothing before it, by construction) a moment
        // ago, so having no bytes behind it is the known-good state, not a hole. This is what
        // happens when two nearby time seeks resolve to the same cue/cluster.
        boolean atFreshWindowStart = position == lastFastSeekStart;
        if (exactTimeMs >= 0
            && cachedAhead >= LOCAL_SEEK_MIN_CONTIGUOUS_BYTES
            && (cachedBehind >= LOCAL_SEEK_MIN_BACKTRACK_BYTES || atFreshWindowStart)) {
            pendingExactSeekTimeMs = -1L;
            readPos = position;
            log.debug("SABR local cached seek: {}ms at byte {} ({} bytes behind, {} bytes ahead{})",
                exactTimeMs, position, cachedBehind, cachedAhead,
                atFreshWindowStart && cachedBehind < LOCAL_SEEK_MIN_BACKTRACK_BYTES ? ", start of last re-aim" : "");
            return;
        }

        // Non-user/container-internal seeks may still reuse any cached byte. They are part of the
        // parser's normal navigation and must not accidentally re-aim the remote SABR timeline.
        if (exactTimeMs < 0 && buffer.hasByte(position)) {
            readPos = position;
            return;
        }

        // Explicit seeks which are not safely cached always re-aim SABR with the exact timestamp.
        // Large non-explicit hard seeks retain the byte->time fallback used by the experimental
        // implementation.
        if (exactTimeMs >= 0 || canFastSeek(position)) {
            beginFastSeek(position, "hard-seek");
            ensureContiguousWindow(position, SEEK_PREFETCH_BYTES);
            if (buffer.hasByte(position)) {
                readPos = position;
                return;
            }
        }

        if (position < evictedBefore) {
            // The bytes were evicted and we could not re-aim SABR; waiting for them would just
            // download the rest of the stream, so fail the seek explicitly.
            throw new IOException("Seek to byte " + position + " is behind the retained SABR window ("
                + evictedBefore + ") and cannot be re-aimed");
        }

        fastSeekActive = false;
        writePos = buffer.contiguousPrefixLength();
        ensureAvailable(position + 1);
        readPos = buffer.hasByte(position) ? position : Math.min(position, buffer.contiguousPrefixLength());
    }

    private void beginFastSeek(long position, @NotNull String reason) {
        long exactTimeMs = pendingExactSeekTimeMs;
        pendingExactSeekTimeMs = -1L;
        // For an exact user seek, use the exact requested playback timestamp. Do not subtract a
        // fixed preroll: placing media fetched from an earlier timestamp at the byte offset chosen
        // for the target cue can misalign the logical Matroska stream. The container itself handles
        // frame filtering around its selected cue point.
        long requestTimeMs = exactTimeMs >= 0
            ? Math.max(0L, exactTimeMs - EXACT_SEEK_PREROLL_MS)
            : estimateTimeForBytePosition(position);

        log.debug("SABR fast seek ({}): byte {} -> request {}ms, target {}ms{} " +
                "(read {}, buffered max {}, downloaded time {}ms)",
            reason, position, requestTimeMs, exactTimeMs >= 0 ? exactTimeMs : requestTimeMs,
            exactTimeMs >= 0 ? " exact" : " estimated",
            readPos, buffer.length(), downloadedDurationMs);

        downloadedDurationMs = requestTimeMs;
        writePos = position;
        lastFastSeekStart = position;
        // Never mix stale bytes from an older sparse neighbourhood with a newly fetched cluster.
        // Overlapping old chunks were a source of invalid EBML boundaries after backwards seeks.
        buffer.truncateFrom(position);
        if (position < initSegmentLength) {
            // The init segment was (partly) cut off; accept it again if the server re-sends it.
            initSegmentLength = 0L;
        }
        consecutiveEmptyResponses = 0;
        fastSeekActive = true;
        finished = false;

        // Segment numbers are local to the requested SABR neighbourhood. Keeping the de-duplication
        // set across a backwards re-aim can cause valid returned segments to be discarded, leaving
        // the Matroska reader at a sparse boundary and eventually producing EOFException.
        downloadedSegments.clear();
        cachedBufferedRanges = Collections.emptyList();
        lastMediaHeaders.clear();
        partialSegmentQueue.clear();
    }

    private boolean canFastSeek(long position) {
        if (contentLength <= 0 || durationMs <= 0 || durationMs == Long.MAX_VALUE) {
            return false;
        }
        if (buffer.hasByte(position)) {
            return false;
        }

        long distance = position >= readPos ? position - readPos : readPos - position;
        // Explicit exact seeks are handled by seekHard() before this method is called. This method
        // is therefore only the fallback for large container/internal hard seeks.
        return distance >= FAST_SEEK_MIN_GAP;
    }

    private long estimateTimeForBytePosition(long position) {
        double fraction = Math.min(1.0d, Math.max(0.0d, (double) position / (double) contentLength));
        long estimated = (long) (fraction * durationMs);
        long target = Math.max(0L, estimated - 1500L);
        long latestSafeStart = Math.max(0L, durationMs - FAST_SEEK_END_MARGIN_MS);
        return Math.min(target, latestSafeStart);
    }

    @Override
    public boolean canSeekHard() {
        return true;
    }

    @Override
    public List<AudioTrackInfoProvider> getTrackInfoProviders() {
        return Collections.emptyList();
    }

    @Override
    public int read() throws IOException {
        if (!ensureAvailable(readPos + 1)) {
            return -1;
        }

        int result = buffer.get(readPos) & 0xFF;
        readPos++;
        return result;
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        if (!ensureAvailable(readPos + 1)) {
            return -1;
        }

        int available = (int) Math.min((long) len, buffer.contiguousAvailable(readPos));
        if (available <= 0) {
            return -1;
        }

        buffer.copyTo(readPos, b, off, available);
        readPos += available;
        return available;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }

        if (!ensureAvailable(readPos + 1)) {
            return 0;
        }

        long skipped = Math.min(n, buffer.contiguousAvailable(readPos));
        readPos += skipped;
        return skipped;
    }

    private boolean closed;

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // The SABR cache belongs to this track only. Drop all retained media and per-stream
        // bookkeeping as soon as Lavaplayer closes the track so the JVM can reclaim the RAM
        // before/while the next track starts. The HttpInterface lifecycle is still owned by
        // the caller and must not be closed here.
        buffer.clear();
        downloadedSegments.clear();
        lastMediaHeaders.clear();
        partialSegmentQueue.clear();
        sabrContextValues.clear();
        activeSabrContextTypes.clear();
        cachedBufferedRanges = null;
        playbackCookie = null;
        pendingExactSeekTimeMs = -1L;
        readPos = 0L;
        writePos = 0L;
        initSegmentLength = 0L;
        evictedBefore = 0L;
        lastFastSeekStart = -1L;
        fastSeekActive = false;
        finished = true;

        log.debug("SABR stream closed; per-track media cache released");
    }
    //</editor-fold>

    /**
     * Ensures at least {@code target} bytes have been downloaded (or the stream has finished),
     * driving SABR requests as needed.
     *
     * @return {@code true} if a byte is available at the current read position.
     */
    private boolean ensureAvailable(long target) throws IOException {
        long wanted = Math.max(0, target - 1);

        while (!finished && !buffer.hasByte(wanted)) {
            evictPlayedMedia();
            fetchNextBatch();
        }

        return buffer.hasByte(readPos);
    }

    /**
     * Drops media far behind the read position. Only called from the read path (where readPos is
     * authoritative) and just before downloading more, so memory is bounded by the window.
     */
    private void evictPlayedMedia() {
        long keepFrom = readPos - MAX_RETAINED_BEHIND_BYTES;
        if (keepFrom <= initSegmentLength) {
            return;
        }

        long evictedEnd = buffer.evictBefore(keepFrom, initSegmentLength);
        if (evictedEnd > 0) {
            evictedBefore = Math.max(evictedBefore, evictedEnd);
        }
    }

    private void ensureContiguousWindow(long position, long minimumBytes) throws IOException {
        long target = Math.max(1L, minimumBytes);
        while (!finished && buffer.contiguousAvailable(position) < target) {
            long before = buffer.contiguousAvailable(position);
            fetchNextBatch();
            long after = buffer.contiguousAvailable(position);
            if (after <= before && finished) {
                break;
            }
        }
    }

    private void fetchNextBatch() throws IOException {
        if (downloadedDurationMs >= durationMs || requestCount >= MAX_REQUESTS) {
            finished = true;
            return;
        }

        long storedBefore = storedSegmentCount;
        long durationBefore = downloadedDurationMs;
        boolean redirected = executeRequestWithRetry();

        boolean progress = redirected
            || storedSegmentCount != storedBefore
            || downloadedDurationMs != durationBefore;

        if (progress) {
            consecutiveEmptyResponses = 0;
        } else if (++consecutiveEmptyResponses >= MAX_CONSECUTIVE_EMPTY_RESPONSES) {
            // Repeated responses without media: assume the stream has really ended.
            finished = true;
        } else {
            // The server may only have sent a policy asking us to wait; honour it before asking again.
            long delay = policyBackoffMs > 0 ? policyBackoffMs : EMPTY_RESPONSE_DELAY_MS;
            sleepInterruptibly(Math.min(delay, MAX_BACKOFF_MS));
        }

        if (downloadedDurationMs >= durationMs) {
            finished = true;
        }
    }

    private static void sleepInterruptibly(long millis) throws IOException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException("Interrupted while waiting for SABR request");
            // Lavaplayer looks for an InterruptedException cause to tell "track stopped" from a failure.
            interrupted.initCause(e);
            throw interrupted;
        }
    }

    private boolean executeRequestWithRetry() throws IOException {
        IOException lastError = null;

        for (int attempt = 0; attempt <= MAX_REQUEST_RETRIES; attempt++) {
            try {
                return executeRequest();
            } catch (SabrFatalException e) {
                throw new IOException(e.getMessage(), e);
            } catch (IOException e) {
                lastError = e;
                log.debug("SABR request attempt {} failed: {}", attempt + 1, e.getMessage());
            }

            if (attempt < MAX_REQUEST_RETRIES) {
                sleepInterruptibly(RETRY_DELAY_MS * (attempt + 1));
            }
        }

        throw lastError != null ? lastError : new IOException("SABR request failed");
    }

    /**
     * Performs a single SABR request/response cycle.
     *
     * @return {@code true} if the response only redirected us to a new URL (no media).
     */
    private boolean executeRequest() throws IOException {
        partialSegmentQueue.clear();
        policyBackoffMs = 0L;
        requestCount++;

        if (cachedBufferedRanges == null) {
            cachedBufferedRanges = buildBufferedRanges();
        }

        byte[] body = buildRequestBody();
        URI url = withRequestNumber(serverAbrStreamingUrl, requestNumber++);

        HttpPost request = new HttpPost(url);
        request.setHeader("Content-Type", "application/x-protobuf");
        request.setHeader("Accept", "application/vnd.yt-ump");
        request.setEntity(new ByteArrayEntity(body));

        byte[] responseBytes;
        int statusCode;

        try (CloseableHttpResponse response = httpInterface.execute(request)) {
            statusCode = response.getStatusLine().getStatusCode();

            if (!HttpClientTools.isSuccessWithContent(statusCode)) {
                throw new IOException("Not success status code: " + statusCode);
            }

            responseBytes = EntityUtils.toByteArray(response.getEntity());
        }

        if (responseBytes == null || responseBytes.length == 0) {
            throw new IOException("Received empty SABR response from server");
        }

        String urlBefore = serverAbrStreamingUrl.toString();
        long bytesBefore = buffer.length();

        try {
            new UmpReader(responseBytes).read(this::handlePart);
        } catch (SabrFatalException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to parse SABR response", e);
        }

        boolean redirected = !serverAbrStreamingUrl.toString().equals(urlBefore) && buffer.length() == bytesBefore;

        // Once media headers have been processed the cached buffered ranges are stale and must be rebuilt.
        if (buffer.length() != bytesBefore) {
            cachedBufferedRanges = null;
        }

        return redirected;
    }

    //<editor-fold desc="UMP part handling">
    private void handlePart(int type, @NotNull byte[] data) {
        try {
            dispatchPart(type, data);
        } catch (SabrFatalException e) {
            throw e;
        } catch (RuntimeException e) {
            // A single malformed metadata part should not abort the whole stream. Media parts are
            // handled separately (a mismatched segment is dropped and re-requested).
            log.debug("Failed to handle SABR part type {} ({} bytes): {} [{}]",
                type, data.length, e.getMessage(), hexPreview(data));
        }
    }

    private void dispatchPart(int type, @NotNull byte[] data) {
        switch (type) {
            case UmpReader.FORMAT_INITIALIZATION_METADATA:
                handleFormatInitialization(data);
                break;
            case UmpReader.NEXT_REQUEST_POLICY:
                handleNextRequestPolicy(data);
                break;
            case UmpReader.SABR_REDIRECT:
                handleSabrRedirect(data);
                break;
            case UmpReader.SABR_ERROR:
                handleSabrError(data);
                break;
            case UmpReader.SABR_CONTEXT_UPDATE:
                handleSabrContextUpdate(data);
                break;
            case UmpReader.SABR_CONTEXT_SENDING_POLICY:
                handleSabrContextSendingPolicy(data);
                break;
            case UmpReader.STREAM_PROTECTION_STATUS:
                handleStreamProtectionStatus(data);
                break;
            case UmpReader.RELOAD_PLAYER_RESPONSE:
                throw new SabrFatalException("Player response reload requested by server");
            case UmpReader.MEDIA_HEADER:
                handleMediaHeader(data);
                break;
            case UmpReader.MEDIA:
                handleMedia(data);
                break;
            case UmpReader.MEDIA_END:
                handleMediaEnd(data);
                break;
            default:
                break;
        }
    }

    private void handleFormatInitialization(@NotNull byte[] data) {
        FormatInitializationMetadata metadata = FormatInitializationMetadata.parse(new ProtoReader(data));
        formatInitialized = true;

        long computed = metadata.durationMs();
        if (computed > 0) {
            durationMs = computed;
        }

        log.debug("Initialized SABR format {} ({})", metadata.formatKey(), metadata.mimeType);
    }

    private void handleNextRequestPolicy(@NotNull byte[] data) {
        ProtoReader reader = new ProtoReader(data);
        int tag;
        while ((tag = reader.readTag()) != -1) {
            switch (reader.getFieldNumber()) {
                case 4:
                    // backoff_time_ms: only honoured when a response carried no media.
                    policyBackoffMs = reader.readVarint();
                    break;
                case 7:
                    // playback_cookie, stored as raw bytes to echo back in the next request.
                    playbackCookie = reader.readBytes();
                    break;
                default:
                    reader.skip();
                    break;
            }
        }
    }

    private void handleSabrRedirect(@NotNull byte[] data) {
        ProtoReader reader = new ProtoReader(data);
        int tag;
        while ((tag = reader.readTag()) != -1) {
            if (reader.getFieldNumber() == 1) {
                String url = reader.readString();
                try {
                    serverAbrStreamingUrl = new URI(url);
                    log.debug("SABR redirect to {}", url);
                } catch (URISyntaxException e) {
                    log.warn("Received invalid SABR redirect URL: {}", url);
                }
            } else {
                reader.skip();
            }
        }
    }

    private void handleSabrError(@NotNull byte[] data) {
        ProtoReader reader = new ProtoReader(data);
        String errorType = null;
        int code = 0;

        int tag;
        while ((tag = reader.readTag()) != -1) {
            switch (reader.getFieldNumber()) {
                case 1:
                    errorType = reader.readString();
                    break;
                case 2:
                    code = (int) reader.readVarint();
                    break;
                default:
                    reader.skip();
                    break;
            }
        }

        throw new SabrFatalException("SABR error: " + errorType + " (" + code + ")");
    }

    private void handleStreamProtectionStatus(@NotNull byte[] data) {
        ProtoReader reader = new ProtoReader(data);
        int tag;
        while ((tag = reader.readTag()) != -1) {
            if (reader.getFieldNumber() == 1) {
                streamProtectionStatus = (int) reader.readVarint();
            } else {
                reader.skip();
            }
        }

        if (streamProtectionStatus == 3) {
            throw new SabrFatalException("Cannot proceed with SABR stream: attestation required (poToken needed)");
        } else if (streamProtectionStatus == 2) {
            log.debug("SABR attestation pending");
        }
    }

    private void handleSabrContextUpdate(@NotNull byte[] data) {
        ProtoReader reader = new ProtoReader(data);
        int type = -1;
        byte[] value = null;
        int writePolicy = 0;
        boolean sendByDefault = false;

        int tag;
        while ((tag = reader.readTag()) != -1) {
            switch (reader.getFieldNumber()) {
                case 1:
                    type = (int) reader.readVarint();
                    break;
                case 3:
                    value = reader.readBytes();
                    break;
                case 4:
                    sendByDefault = reader.readVarint() != 0;
                    break;
                case 5:
                    writePolicy = (int) reader.readVarint();
                    break;
                default:
                    reader.skip();
                    break;
            }
        }

        if (type == -1 || value == null || value.length == 0) {
            return;
        }

        // KEEP_EXISTING == 2: don't overwrite an existing context of this type.
        if (writePolicy == 2 && sabrContextValues.containsKey(type)) {
            return;
        }

        sabrContextValues.put(type, value);

        if (sendByDefault) {
            activeSabrContextTypes.add(type);
        }
    }

    private void handleSabrContextSendingPolicy(@NotNull byte[] data) {
        ProtoReader reader = new ProtoReader(data);
        int tag;
        while ((tag = reader.readTag()) != -1) {
            int value = (int) reader.readVarint();
            switch (reader.getFieldNumber()) {
                case 1:
                    activeSabrContextTypes.add(value);
                    break;
                case 2:
                    activeSabrContextTypes.remove(value);
                    break;
                case 3:
                    sabrContextValues.remove(value);
                    activeSabrContextTypes.remove(value);
                    break;
                default:
                    break;
            }
        }
    }

    private void handleMediaHeader(@NotNull byte[] data) {
        MediaHeader header = MediaHeader.parse(new ProtoReader(data));
        int segmentNumber = header.segmentNumber();

        if (downloadedSegments.contains(segmentNumber)) {
            // Already have this segment; a duplicate header can be safely ignored.
            return;
        }

        partialSegmentQueue.put(header.headerId, new PartialSegment(header, segmentNumber));
    }

    private void handleMedia(@NotNull byte[] data) {
        if (data.length < 1) {
            return;
        }

        int headerId = data[0] & 0xFF;
        PartialSegment segment = partialSegmentQueue.get(headerId);

        if (segment == null) {
            return;
        }

        segment.data.write(data, 1, data.length - 1);
    }

    private void handleMediaEnd(@NotNull byte[] data) {
        if (data.length < 1) {
            return;
        }

        int headerId = data[0] & 0xFF;
        PartialSegment segment = partialSegmentQueue.remove(headerId);

        if (segment == null) {
            return;
        }

        byte[] segmentBytes = segment.data.toByteArray();

        if (segment.header.contentLength >= 0 && segmentBytes.length != segment.header.contentLength) {
            log.warn("SABR segment {} content length mismatch (expected {}, got {}); discarding",
                segment.segmentNumber, segment.header.contentLength, segmentBytes.length);
            return;
        }

        if (segment.header.isInitSeg) {
            if (initSegmentLength > 0) {
                // The init segment is already at the start of the buffer. Writing it again at
                // writePos (e.g. after a re-aim) would corrupt the container stream.
                log.debug("Dropping duplicate SABR init segment ({} bytes)", segmentBytes.length);
                return;
            }
            buffer.put(0L, segmentBytes);
            initSegmentLength = segmentBytes.length;
            if (!fastSeekActive) {
                writePos = buffer.length();
            }
        } else if (fastSeekActive) {
            buffer.put(writePos, segmentBytes);
            writePos += segmentBytes.length;
        } else {
            buffer.append(segmentBytes);
            writePos = buffer.length();
        }
        storedSegmentCount++;
        downloadedSegments.add(segment.segmentNumber);
        downloadedDurationMs += segment.header.effectiveDurationMs();
        lastMediaHeaders.add(segment.header);
    }
    //</editor-fold>

    //<editor-fold desc="Request building">
    private byte[] buildRequestBody() {
        ProtoWriter request = new ProtoWriter();

        // client_abr_state (field 1)
        ProtoWriter abrState = new ProtoWriter();
        abrState.writeVarint(28, Math.max(0, downloadedDurationMs)); // player_time_ms
        abrState.writeFloat(35, 1.0f); // playback_rate
        abrState.writeVarint(34, 1); // visibility
        abrState.writeVarint(40, ENABLED_TRACK_TYPES_AUDIO_ONLY); // enabled_track_types_bitfield
        if (drcEnabled) {
            abrState.writeBool(46, true); // drc_enabled
        }
        request.writeMessage(1, abrState);

        // selected_format_ids (field 2) - only sent once the format has been initialized.
        if (formatInitialized) {
            audioFormatId.writeTo(request, 2);
        }

        // buffered_ranges (field 3)
        if (cachedBufferedRanges != null) {
            for (BufferedRangeData range : cachedBufferedRanges) {
                writeBufferedRange(request, range);
            }
        }

        // video_playback_ustreamer_config (field 5)
        request.writeBytes(5, ustreamerConfig);

        // preferred_audio_format_ids (field 16)
        audioFormatId.writeTo(request, 16);

        // streamer_context (field 19)
        request.writeMessage(19, buildStreamerContext());

        return request.toByteArray();
    }

    private void writeBufferedRange(@NotNull ProtoWriter request, @NotNull BufferedRangeData range) {
        ProtoWriter br = new ProtoWriter();
        range.formatId.writeTo(br, 1);
        br.writeVarint(2, range.startTimeMs);
        br.writeVarint(3, range.durationMs);
        br.writeVarint(4, range.startSegmentIndex);
        br.writeVarint(5, range.endSegmentIndex);
        request.writeMessage(3, br);
    }

    private ProtoWriter buildStreamerContext() {
        ProtoWriter context = new ProtoWriter();

        // client_info (field 1)
        clientInfo.writeTo(context, 1);

        // po_token (field 2)
        if (poToken != null) {
            context.writeBytes(2, poToken);
        }

        // playback_cookie (field 3)
        if (playbackCookie != null) {
            context.writeBytes(3, playbackCookie);
        }

        // sabr_contexts (field 5) / unsent_sabr_contexts (field 6)
        for (Map.Entry<Integer, byte[]> entry : sabrContextValues.entrySet()) {
            if (activeSabrContextTypes.contains(entry.getKey())) {
                ProtoWriter sabrContext = new ProtoWriter();
                sabrContext.writeVarint(1, entry.getKey());
                sabrContext.writeBytes(2, entry.getValue());
                context.writeMessage(5, sabrContext);
            } else {
                context.writeVarint(6, entry.getKey());
            }
        }

        return context;
    }

    @NotNull
    private List<BufferedRangeData> buildBufferedRanges() {
        List<BufferedRangeData> ranges = new ArrayList<>();

        if (!lastMediaHeaders.isEmpty()) {
            long duration = 0;
            for (MediaHeader header : lastMediaHeaders) {
                duration += header.effectiveDurationMs();
            }

            MediaHeader first = lastMediaHeaders.get(0);
            MediaHeader last = lastMediaHeaders.get(lastMediaHeaders.size() - 1);

            BufferedRangeData range = new BufferedRangeData();
            range.formatId = audioFormatId;
            range.startTimeMs = first.startMs;
            range.durationMs = duration;
            range.startSegmentIndex = Math.max(1, first.sequenceNumber);
            range.endSegmentIndex = Math.max(1, last.sequenceNumber);
            ranges.add(range);

            lastMediaHeaders.clear();
        }

        return ranges;
    }

    @NotNull
    private static String hexPreview(@NotNull byte[] data) {
        int count = Math.min(data.length, 64);
        StringBuilder sb = new StringBuilder(count * 2);
        for (int i = 0; i < count; i++) {
            sb.append(Character.forDigit((data[i] >> 4) & 0xF, 16));
            sb.append(Character.forDigit(data[i] & 0xF, 16));
        }
        if (data.length > count) {
            sb.append("...");
        }
        return sb.toString();
    }

    @NotNull
    private static URI withRequestNumber(@NotNull URI url, int requestNumber) throws IOException {
        try {
            return new URIBuilder(url).setParameter("rn", Integer.toString(requestNumber)).build();
        } catch (URISyntaxException e) {
            throw new IOException("Failed to build SABR request URL", e);
        }
    }
    //</editor-fold>

    private static class PartialSegment {
        final MediaHeader header;
        final int segmentNumber;
        final ByteArrayOutputStream data = new ByteArrayOutputStream();

        PartialSegment(@NotNull MediaHeader header, int segmentNumber) {
            this.header = header;
            this.segmentNumber = segmentNumber;
        }
    }

    private static class BufferedRangeData {
        FormatId formatId;
        long startTimeMs;
        long durationMs;
        int startSegmentIndex;
        int endSegmentIndex;
    }

    /**
     * Signals a non-recoverable SABR protocol condition that should abort the current stream
     * without retrying (e.g. an explicit server error or a required attestation we cannot satisfy).
     */
    private static class SabrFatalException extends RuntimeException {
        SabrFatalException(String message) {
            super(message);
        }
    }

    /**
     * A growable byte buffer composed of discrete chunks, supporting random-access reads. Avoids the
     * repeated full-array reallocation of a single backing array as the stream grows.
     */
    private static class ChunkedByteBuffer {
        // Chunks are keyed by their absolute byte offset. This allows a forward SABR seek to cache
        // media around the requested position without downloading the entire gap first.
        private final NavigableMap<Long, byte[]> chunks = new TreeMap<>();
        private long length;

        void append(@NotNull byte[] chunk) {
            put(length, chunk);
        }

        void put(long position, @NotNull byte[] chunk) {
            if (chunk.length == 0) {
                return;
            }
            chunks.put(position, chunk);
            length = Math.max(length, position + chunk.length);
        }

        long length() {
            return length;
        }

        void clear() {
            chunks.clear();
            length = 0L;
        }

        boolean hasByte(long position) {
            if (position < 0) {
                return false;
            }
            Map.Entry<Long, byte[]> entry = chunks.floorEntry(position);
            return entry != null && position - entry.getKey() < entry.getValue().length;
        }

        long contiguousAvailable(long position) {
            if (position < 0) {
                return 0;
            }

            Map.Entry<Long, byte[]> entry = chunks.floorEntry(position);
            if (entry == null || position - entry.getKey() >= entry.getValue().length) {
                return 0;
            }

            long end = entry.getKey() + entry.getValue().length;
            Map.Entry<Long, byte[]> next = chunks.higherEntry(entry.getKey());
            while (next != null && next.getKey() <= end) {
                end = Math.max(end, next.getKey() + next.getValue().length);
                next = chunks.higherEntry(next.getKey());
            }
            return Math.max(0, end - position);
        }

        long contiguousBefore(long position) {
            if (position <= 0) {
                return position == 0 && hasByte(0) ? 0 : 0;
            }

            Map.Entry<Long, byte[]> entry = chunks.floorEntry(position - 1);
            if (entry == null || position - 1 >= entry.getKey() + entry.getValue().length) {
                return 0;
            }

            long start = entry.getKey();
            long cursor = start;
            Map.Entry<Long, byte[]> previous = chunks.lowerEntry(entry.getKey());
            while (previous != null) {
                long previousEnd = previous.getKey() + previous.getValue().length;
                if (previousEnd < cursor) {
                    break;
                }
                start = previous.getKey();
                cursor = start;
                previous = chunks.lowerEntry(previous.getKey());
            }
            return Math.max(0L, position - start);
        }

        long contiguousPrefixLength() {
            long end = 0;
            for (Map.Entry<Long, byte[]> entry : chunks.entrySet()) {
                if (entry.getKey() > end) {
                    break;
                }
                end = Math.max(end, entry.getKey() + entry.getValue().length);
            }
            return end;
        }

        /**
         * Removes whole chunks that end at or before {@code position}, except chunks starting
         * below {@code protectedPrefix} (the init segment).
         *
         * @return the highest end offset of a removed chunk, or 0 if nothing was removed.
         */
        long evictBefore(long position, long protectedPrefix) {
            long highestEnd = 0L;
            Iterator<Map.Entry<Long, byte[]>> it =
                chunks.subMap(Math.max(0L, protectedPrefix), true, position, false).entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<Long, byte[]> entry = it.next();
                long end = entry.getKey() + entry.getValue().length;
                if (end > position) {
                    break;
                }
                highestEnd = Math.max(highestEnd, end);
                it.remove();
            }
            return highestEnd;
        }

        void truncateFrom(long position) {
            if (position < 0) {
                position = 0;
            }

            Map.Entry<Long, byte[]> overlap = chunks.floorEntry(position);
            if (overlap != null) {
                long start = overlap.getKey();
                byte[] data = overlap.getValue();
                long end = start + data.length;
                if (start < position && end > position) {
                    int keep = (int) (position - start);
                    if (keep > 0) {
                        chunks.put(start, Arrays.copyOf(data, keep));
                    } else {
                        chunks.remove(start);
                    }
                }
            }

            chunks.tailMap(position, true).clear();
            length = 0;
            for (Map.Entry<Long, byte[]> entry : chunks.entrySet()) {
                length = Math.max(length, entry.getKey() + entry.getValue().length);
            }
        }

        byte get(long position) {
            Map.Entry<Long, byte[]> entry = chunks.floorEntry(position);
            if (entry == null || position - entry.getKey() >= entry.getValue().length) {
                throw new IndexOutOfBoundsException("Position " + position + " is not buffered");
            }
            return entry.getValue()[(int) (position - entry.getKey())];
        }

        void copyTo(long position, @NotNull byte[] dest, int destOffset, int count) {
            int written = 0;
            long cursor = position;

            while (written < count) {
                Map.Entry<Long, byte[]> entry = chunks.floorEntry(cursor);
                if (entry == null || cursor - entry.getKey() >= entry.getValue().length) {
                    break;
                }

                byte[] chunk = entry.getValue();
                int chunkOffset = (int) (cursor - entry.getKey());
                int toCopy = Math.min(count - written, chunk.length - chunkOffset);
                System.arraycopy(chunk, chunkOffset, dest, destOffset + written, toCopy);
                written += toCopy;
                cursor += toCopy;

                if (written < count && !hasByte(cursor)) {
                    break;
                }
            }
        }
    }
}
