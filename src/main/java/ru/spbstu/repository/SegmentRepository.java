package ru.spbstu.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.spbstu.model.SegmentMetadata;

@Repository
public interface SegmentRepository extends JpaRepository<SegmentMetadata, String> {
}
