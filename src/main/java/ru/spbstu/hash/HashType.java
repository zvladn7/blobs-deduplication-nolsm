package ru.spbstu.hash;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public enum HashType {
    SHA256("SHA-256"),
    SHA512("SHA-512"),
    MD5("MD5");

    private final String algorithm;

    HashType(@NotNull String algorithm) {
        this.algorithm = Objects.requireNonNull(algorithm);
    }

    @NotNull
    public String getAlgorithm() {
        return algorithm;
    }

}
