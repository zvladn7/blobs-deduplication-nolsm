package ru.spbstu.hash;

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SegmentHashUtil {

    private SegmentHashUtil() {}

    @NotNull
    public static List<MemorySegmentWithHash> calculateHashes(@NotNull List<MemorySegment> fileByteSegments,
                                                              @NotNull HashType hashType) throws NoSuchAlgorithmException {
        Objects.requireNonNull(fileByteSegments);
        Objects.requireNonNull(hashType);
        MessageDigest md5 = MessageDigest.getInstance(hashType.getAlgorithm());
        List<MemorySegmentWithHash> results = new ArrayList<>(fileByteSegments.size());
        for (MemorySegment fileByteSegment : fileByteSegments) {
            md5.update(fileByteSegment.toArray(ValueLayout.OfByte.JAVA_BYTE));
            byte[] digest = md5.digest();
            String hash = bytesToHex(digest);
            results.add(new MemorySegmentWithHash(hash, fileByteSegment));
        }
        return results;
    }

    @NotNull
    private static String bytesToHex(@NotNull byte[] bytes) {
        Objects.requireNonNull(bytes);
        StringBuilder builder = new StringBuilder();
        for (var b : bytes) {
            builder.append(String.format("%02x", b & 0xff));
        }
        return builder.toString();
    }

}
