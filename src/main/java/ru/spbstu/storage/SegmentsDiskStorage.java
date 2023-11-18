package ru.spbstu.storage;

import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import ru.spbstu.exception.StorageException;
import ru.spbstu.hash.SegmentHasherResult;
import ru.spbstu.model.SegmentMetadata;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class SegmentsDiskStorage {

    private static final String COMPRESSED_FILE_POSTFIX = ".bin";

    private final Path segmentsStoragePath;
    private final Path compressedDataPath;
    private final Path decompressedDataPath;
    private final Arena arena;
    private volatile List<MemorySegment> segmentList;

    public SegmentsDiskStorage() throws IOException {
        this.segmentsStoragePath = Files.createTempDirectory("segments_dao");
        this.compressedDataPath = Files.createTempDirectory("compressed_data");
        this.decompressedDataPath = Files.createTempDirectory("decompressed_data");
        this.arena = Arena.ofShared();
        this.segmentList = load(segmentsStoragePath, arena);
    }

    public static void main(String[] args) throws IOException {
        Path file = Files.createFile(Path.of("/test_dir"));
        Files.createDirectories(file);
        System.out.println(file.toAbsolutePath().toString());
        Files.delete(file);
    }

    public static List<MemorySegment> load(@NotNull Path segmentsStoragePath,
                                           @NotNull Arena arena) throws IOException {

        Path indexTmp = segmentsStoragePath.resolve("index.tmp");
        Path indexFile = segmentsStoragePath.resolve("index.idx");

        if (!Files.exists(indexFile)) {
            if (Files.exists(indexTmp)) {
                Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.createFile(indexFile);
            }
        }

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        List<MemorySegment> results = new ArrayList<>();
        for (String fileName : existedFiles) {
            Path filePath = segmentsStoragePath.resolve(fileName);
            try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                results.add(fileChannel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        Files.size(filePath),
                        arena
                ));
            }
        }
        return results;
    }

    public Map<String, SegmentMetadata> saveSegmentsOnDisk(@NotNull Collection<SegmentMetadata> segmentMetadatas,
                                                           @NotNull Map<String, MemorySegment> hashToBytesSegmentMap) throws IOException {
        Path indexTmp = segmentsStoragePath.resolve("index.tmp");
        Path indexFile = segmentsStoragePath.resolve("index.idx");

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it's ok actually it's normal state
        }

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        String newFileName = String.valueOf(existedFiles.size());

        long dataSize = 0;
        for (SegmentMetadata segmentMetadata : segmentMetadatas) {
            String hash = segmentMetadata.getHash();
            MemorySegment memorySegment = hashToBytesSegmentMap.get(hash);
            if (memorySegment == null) {
                throw new StorageException(String.format("No bytes for segment with hash: %s", hash));
            }
            dataSize += memorySegment.byteSize();
        }

        Map<String, SegmentMetadata> result = new HashMap<>();

        try (FileChannel fileChannel = FileChannel.open(
                segmentsStoragePath.resolve(newFileName),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
             Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    dataSize,
                    writeArena
            );

            long dataOffset = 0;
            for (SegmentMetadata segmentMetadata : segmentMetadatas) {
                String hash = segmentMetadata.getHash();
                MemorySegment nextSegment = hashToBytesSegmentMap.get(hash);
                MemorySegment.copy(nextSegment, 0, fileSegment, 0, nextSegment.byteSize());

                segmentMetadata.setFileName(newFileName);
                segmentMetadata.setOffset(dataOffset);
                result.put(hash, segmentMetadata);

                dataOffset += nextSegment.byteSize();
            }
        }

        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(newFileName);
        Files.write(
                indexTmp,
                list,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.deleteIfExists(indexFile);

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);

        this.segmentList = load(segmentsStoragePath, arena);

        return result;
    }

    /**
     * compressed file format:
     * |segment size in bytes|segments count|segment0|segment1|segment2|...
     */
    public void saveCompressedDataFile(@NotNull String fileName,
                                       @NotNull List<SegmentHasherResult> segmentHasherResults,
                                       @NotNull Map<String, SegmentMetadata> segmentsToUpdateInDBMap) throws IOException {
        int compressedDataSize = segmentHasherResults.size() * Integer.BYTES;

        try (FileChannel fileChannel = FileChannel.open(
                compressedDataPath.resolve(fileName + COMPRESSED_FILE_POSTFIX),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
             Arena writeArena = Arena.ofConfined();
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    compressedDataSize,
                    writeArena
            );

            int dataOffset = 0;
            long segmentSizeInBytes = segmentHasherResults.get(0).getMemorySegment().byteSize();
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset, segmentSizeInBytes);
            dataOffset += Long.BYTES;

            int segmentsCount = segmentHasherResults.size();
            fileSegment.set(ValueLayout.JAVA_INT_UNALIGNED, dataOffset, segmentsCount);
            dataOffset += Integer.BYTES;

            long fileSizeInBytes = segmentHasherResults.stream()
                    .map(SegmentHasherResult::getMemorySegment)
                    .map(MemorySegment::byteSize)
                    .mapToLong(Long::longValue)
                    .sum();
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset, fileSizeInBytes);
            dataOffset += Long.BYTES;

            for (SegmentHasherResult hasherResult : segmentHasherResults) {
                String hash = hasherResult.getHash();
                SegmentMetadata metadata = segmentsToUpdateInDBMap.get(hash);
                int segmentId = metadata.getId();
                fileSegment.set(ValueLayout.JAVA_INT_UNALIGNED, dataOffset, segmentId);
                dataOffset += Integer.BYTES;
            }
        }
    }

    public CompressedFileInfo readCompressedFileInfo(@NotNull String fileName) throws IOException {
        Path compressedFilePath = compressedDataPath.resolve(fileName + COMPRESSED_FILE_POSTFIX);
        try (FileChannel fileChannel = FileChannel.open(compressedFilePath, StandardOpenOption.READ);
             Arena arena = Arena.ofConfined();
        ) {
            MemorySegment memorySegment
                    = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(compressedFilePath), arena);

            long segmentSizeInBytes = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            int segmentsCount = memorySegment.get(ValueLayout.JAVA_INT_UNALIGNED, Long.BYTES);
            long fileSizeInBytes = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + Integer.BYTES);
            List<Integer> metadataIds = new ArrayList<>(segmentsCount);
            int baseOffset = 2 * Long.BYTES + Integer.BYTES;
            for (int offsetIdx = 0; offsetIdx < segmentsCount; ++offsetIdx) {
                int dataOffset = baseOffset + offsetIdx * Integer.BYTES;
                int metadataId = memorySegment.get(ValueLayout.JAVA_INT_UNALIGNED, dataOffset);
                metadataIds.add(metadataId);
            }
            if (segmentsCount != metadataIds.size()) {
                throw new StorageException(
                        String.format("Segments count and metadata size isn't equal on data read, file: %s", fileChannel));
            }
            return new CompressedFileInfo(compressedFilePath.getFileName().toString(), segmentSizeInBytes, segmentsCount, fileSizeInBytes, metadataIds);
        }
    }

    public void decompressStoredFile(@NotNull CompressedFileInfo compressedFileInfo,
                                     @NotNull Map<Integer, SegmentMetadata> idToMetadataMap) {
        String s = compressedFileInfo.compressedFileName();
        List<Integer> metadataIds = compressedFileInfo.metadataIds();
        long segmentSizeInBytes = compressedFileInfo.segmentSizeInBytes();
        long fileSizeInBytes = compressedFileInfo.fileSizeInBytes();

//        try (FileChannel fileChannel = FileChannel.open(
//
//        )) {
//
//        }
    }


    public Map<String, SegmentMetadata> calculateSegmentsToUpdateInDB(@NotNull Collection<String> fileHashes,
                                                                      @NotNull Map<String, SegmentMetadata> segmentsFromDBMap) {
        Map<String, SegmentMetadata> segmentsFromCurrentFileMap = new HashMap<>(fileHashes.size() * 2);
        Map<String, SegmentMetadata> segmentsToSaveMap = new HashMap<>(fileHashes.size() * 2);

        int reusedFromDBSegments = 0;
        int duplicateSegments = 0;
        for (String segmentHash : fileHashes) {
            SegmentMetadata segmentFromDB = segmentsFromDBMap.get(segmentHash);
            // Сегмент уже сохранен на диске
            if (segmentFromDB != null) {
                segmentsToSaveMap.put(segmentHash, new SegmentMetadata(segmentFromDB.getId(), segmentHash, segmentFromDB.getFileName(),
                        segmentFromDB.getOffset(), segmentFromDB.getReferenceCount() + 1));
                reusedFromDBSegments++;
                duplicateSegments++;
                continue;
            }

            SegmentMetadata segmentFromCurrentFile = segmentsFromCurrentFileMap.get(segmentHash);
            if (segmentFromCurrentFile != null) {
                segmentFromCurrentFile = new SegmentMetadata(segmentHash, segmentFromCurrentFile.getFileName(),
                        segmentFromCurrentFile.getOffset(), segmentFromCurrentFile.getReferenceCount() + 1);
                segmentsToSaveMap.put(segmentHash, segmentFromCurrentFile);
                segmentsFromCurrentFileMap.put(segmentHash, segmentFromCurrentFile);
                duplicateSegments++;
                continue;
            }

            segmentsToSaveMap.put(segmentHash, new SegmentMetadata(segmentHash));
        }
        return segmentsToSaveMap;
    }

}
