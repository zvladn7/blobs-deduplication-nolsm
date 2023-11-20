package ru.spbstu.service;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.exception.StorageException;
import ru.spbstu.hash.MemorySegmentWithHash;
import ru.spbstu.model.SegmentMetadata;
import ru.spbstu.storage.compressed.CompressedFilesDiskStorage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CompressedStorageService {

    private final CompressedFilesDiskStorage diskStorage;

    public CompressedStorageService(@NotNull CompressedFilesDiskStorage diskStorage) {
        this.diskStorage = Objects.requireNonNull(diskStorage);
    }

    public void save(@NotNull Path path,
                     @NotNull List<MemorySegmentWithHash> memorySegmentWithHashes,
                     @NotNull Map<String, SegmentMetadata> allHashToMetadataMap) {
        try {
            diskStorage.saveCompressedDataFile(path.getFileName().toString(), memorySegmentWithHashes, allHashToMetadataMap);
        } catch (IOException e) {
            throw new StorageException(
                    String.format("Fail to save compressed file %s to storage", path.getFileName().toString()));
        }
    }


}
