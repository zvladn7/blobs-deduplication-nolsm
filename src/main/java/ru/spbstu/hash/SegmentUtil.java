package ru.spbstu.hash;

import org.jetbrains.annotations.NotNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class SegmentUtil {

    private static final int SEGMENT_SIZE_IN_BYTES = 4;

    private SegmentUtil() {}

    public static List<MemorySegment> getSegmentsOfBytes(@NotNull Path path) throws IOException {
        int fileSegmentsCount = getFileSegmentsCount(Files.size(path));
        List<MemorySegment> fileSegments = new ArrayList<>(fileSegmentsCount);
        try (InputStream inputStream = new FileInputStream(path.toFile())) {
            while (inputStream.available() > 0) {
                byte[] nextSegmentBytes = inputStream.readNBytes(SEGMENT_SIZE_IN_BYTES);
                fileSegments.add(MemorySegment.ofArray(nextSegmentBytes));
            }
        }
        return fileSegments;
    }

    private static int getFileSegmentsCount(long fileSizeInBytes) {
        return (int) Math.ceil((double) fileSizeInBytes / SEGMENT_SIZE_IN_BYTES);
    }

}
