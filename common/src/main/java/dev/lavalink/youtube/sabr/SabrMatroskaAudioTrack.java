package dev.lavalink.youtube.sabr;

import com.sedmelluq.discord.lavaplayer.container.matroska.*;
import com.sedmelluq.discord.lavaplayer.container.matroska.format.MatroskaFileTrack;
import com.sedmelluq.discord.lavaplayer.tools.ExceptionTools;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import com.sedmelluq.discord.lavaplayer.track.BaseAudioTrack;
import com.sedmelluq.discord.lavaplayer.track.playback.AudioProcessingContext;
import com.sedmelluq.discord.lavaplayer.track.playback.LocalAudioTrackExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Audio track that handles the processing of MKV and WEBM formats
 */
public class SabrMatroskaAudioTrack extends BaseAudioTrack {
    private static final Logger log = LoggerFactory.getLogger(SabrMatroskaAudioTrack.class);

    private final SabrStream inputStream;

    /**
     * @param trackInfo   Track info
     * @param inputStream Input stream for the file
     */
    public SabrMatroskaAudioTrack(AudioTrackInfo trackInfo, SabrStream inputStream) {
        super(trackInfo);

        this.inputStream = inputStream;
    }

    @Override
    public void process(LocalAudioTrackExecutor localExecutor) {
        MatroskaTrackConsumer trackConsumer = null;

        try {
            MatroskaStreamingFile file = loadMatroskaFile();
            trackConsumer = loadAudioTrack(file, localExecutor.getProcessingContext());
            MatroskaTrackConsumer finalTrackConsumer = trackConsumer;

            localExecutor.executeProcessingLoop(() -> {
                file.provideFrames(finalTrackConsumer);
            }, position -> {
                inputStream.prepareTimeSeek(position);
                try {
                    file.seekToTimecode(finalTrackConsumer.getTrack().index, position);
                } finally {
                    inputStream.finishTimeSeek();
                }
            });
        } finally {
            try {
                if (trackConsumer != null) {
                    ExceptionTools.closeWithWarnings(trackConsumer);
                }
            } finally {
                // Always release the per-track SABR cache, including on skip/stop.
                inputStream.close();
            }
        }
    }

    private MatroskaStreamingFile loadMatroskaFile() {
        try {
            MatroskaStreamingFile file = new MatroskaStreamingFile(inputStream);
            file.readFile();

            accurateDuration.set((int) file.getDuration());
            return file;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private MatroskaTrackConsumer loadAudioTrack(MatroskaStreamingFile file, AudioProcessingContext context) {
        MatroskaTrackConsumer trackConsumer = null;
        boolean success = false;

        try {
            trackConsumer = selectAudioTrack(file.getTrackList(), context);

            if (trackConsumer == null) {
                throw new IllegalStateException("No supported audio tracks in the file.");
            } else {
                log.debug("Starting to play track with codec {}", trackConsumer.getTrack().codecId);
            }

            trackConsumer.initialise();
            success = true;
        } finally {
            if (!success && trackConsumer != null) {
                ExceptionTools.closeWithWarnings(trackConsumer);
            }
        }

        return trackConsumer;
    }

    private MatroskaTrackConsumer selectAudioTrack(MatroskaFileTrack[] tracks, AudioProcessingContext context) {
        MatroskaTrackConsumer trackConsumer = null;

        for (MatroskaFileTrack track : tracks) {
            if (track.type == MatroskaFileTrack.Type.AUDIO) {
                if (MatroskaContainerProbe.OPUS_CODEC.equals(track.codecId)) {
                    trackConsumer = new MatroskaOpusTrackConsumer(context, track);
                    break;
                } else if (MatroskaContainerProbe.VORBIS_CODEC.equals(track.codecId)) {
                    trackConsumer = new MatroskaVorbisTrackConsumer(context, track);
                } else if (MatroskaContainerProbe.AAC_CODEC.equals(track.codecId)) {
                    trackConsumer = new MatroskaAacTrackConsumer(context, track);
                }
            }
        }

        return trackConsumer;
    }
}
