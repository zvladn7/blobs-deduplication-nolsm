package ru.spbstu.hash;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class SegmentUtil {

    private SegmentUtil() {}

    public static List<MemorySegment> getSegmentsOfBytes(@NotNull Path path, int segmentSizeInBytes) throws IOException {
        int fileSegmentsCount = getFileSegmentsCount(Files.size(path), segmentSizeInBytes);
        List<MemorySegment> fileSegments = new ArrayList<>(fileSegmentsCount);
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(path.toFile()))) {
            while (inputStream.available() > 0) {
                byte[] nextSegmentBytes = inputStream.readNBytes(segmentSizeInBytes);
                fileSegments.add(MemorySegment.ofArray(nextSegmentBytes));
            }
        }
        return fileSegments;
    }

    private static int getFileSegmentsCount(long fileSizeInBytes, int segmentSizeInBytes) {
        return (int) Math.ceil((double) fileSizeInBytes / segmentSizeInBytes);
    }

}
