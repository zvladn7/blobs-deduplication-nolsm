package ru.spbstu.storage.metadata;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.spbstu.model.SegmentMetadata;

@Repository
public interface SegmentMetadataRepository extends JpaRepository<SegmentMetadata, String> {
}
