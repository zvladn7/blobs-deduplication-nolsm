package ru.spbstu.hash;

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class SegmentHashUtil {

    public static List<DiskSegment> calculateHashes(@NotNull List<MemorySegment> fileByteSegments) throws NoSuchAlgorithmException {
        MessageDigest md5 = MessageDigest.getInstance("SHA-256");
        List<DiskSegment> results = new ArrayList<>(fileByteSegments.size());
        for (MemorySegment fileByteSegment : fileByteSegments) {
            md5.update(fileByteSegment.toArray(ValueLayout.OfByte.JAVA_BYTE));
            byte[] digest = md5.digest();
            String hash = bytesToHex(digest);
            results.add(new DiskSegment(hash, fileByteSegment));
        }
        return results;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (var b : bytes) {
            builder.append(String.format("%02x", b & 0xff));
        }
        return builder.toString();
    }

}
