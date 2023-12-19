package ru.spbstu.util;

import ru.spbstu.hash.HashType;

public record Context(HashType hashType, int segmentSizeInBytes) {
}
