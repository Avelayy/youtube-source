package dev.lavalink.youtube.sabr;

import com.sedmelluq.discord.lavaplayer.container.mpeg.*;

import com.sedmelluq.discord.lavaplayer.container.mpeg.reader.MpegFileTrackProvider;
import com.sedmelluq.discord.lavaplayer.tools.ExceptionTools;
import com.sedmelluq.discord.lavaplayer.tools.FriendlyException;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import com.sedmelluq.discord.lavaplayer.track.BaseAudioTrack;
import com.sedmelluq.discord.lavaplayer.track.playback.AudioProcessingContext;
import com.sedmelluq.discord.lavaplayer.track.playback.LocalAudioTrackExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.sedmelluq.discord.lavaplayer.tools.FriendlyException.Severity.FAULT;
import static com.sedmelluq.discord.lavaplayer.tools.FriendlyException.Severity.SUSPICIOUS;

/**
 * Audio track that handles the processing of MP4 format
 */
public class SabrMpegAudioTrack extends BaseAudioTrack {
    private static final Logger log = LoggerFactory.getLogger(SabrMpegAudioTrack.class);

    private final SabrStream inputStream;

    /**
     * @param trackInfo   Track info
     * @param inputStream Input stream for the MP4 file
     */
    public SabrMpegAudioTrack(AudioTrackInfo trackInfo, SabrStream inputStream) {
        super(trackInfo);

        this.inputStream = inputStream;
    }

    @Override
    public void process(LocalAudioTrackExecutor localExecutor) {
        MpegTrackConsumer trackConsumer = null;
        try {
            MpegFileLoader file = new MpegFileLoader(inputStream);
            file.parseHeaders();
            trackConsumer = loadAudioTrack(file, localExecutor.getProcessingContext());
            MpegTrackConsumer finalTrackConsumer = trackConsumer;

            MpegFileTrackProvider fileReader = file.loadReader(finalTrackConsumer);
            if (fileReader == null) {
                throw new FriendlyException("Unknown MP4 format.", SUSPICIOUS, null);
            }

            accurateDuration.set(fileReader.getDuration());
            localExecutor.executeProcessingLoop(fileReader::provideFrames, position -> {
                inputStream.prepareTimeSeek(position);
                try {
                    fileReader.seekToTimecode(position);
                } finally {
                    inputStream.finishTimeSeek();
                }
            });
        } finally {
            try {
                if (trackConsumer != null) {
                    trackConsumer.close();
                }
            } finally {
                // Release this track's SABR cache even if consumer cleanup fails.
                inputStream.close();
            }
        }
    }

    protected MpegTrackConsumer loadAudioTrack(MpegFileLoader file, AudioProcessingContext context) {
        MpegTrackConsumer trackConsumer = null;
        boolean success = false;

        try {
            trackConsumer = selectAudioTrack(file.getTrackList(), context);

            if (trackConsumer == null) {
                StringBuilder error = new StringBuilder();
                error.append("The audio codec used in the track is not supported, options:\n");
                file.getTrackList().forEach(track -> error.append(track.handler).append("|").append(track.codecName).append("\n"));
                throw new FriendlyException(error.toString(), SUSPICIOUS, null);
            } else {
                log.debug("Starting to play track with codec {}", trackConsumer.getTrack().codecName);
            }

            trackConsumer.initialise();
            success = true;
            return trackConsumer;
        } catch (Exception e) {
            throw ExceptionTools.wrapUnfriendlyExceptions("Something went wrong when loading an MP4 format track.", FAULT, e);
        } finally {
            if (!success && trackConsumer != null) {
                trackConsumer.close();
            }
        }
    }

    private MpegTrackConsumer selectAudioTrack(List<MpegTrackInfo> tracks, AudioProcessingContext context) {
        for (MpegTrackInfo track : tracks) {
            if ("soun".equals(track.handler) && "mp4a".equals(track.codecName)) {
                return new MpegAacTrackConsumer(context, track);
            }
        }
        return null;
    }
}
