package ru.spbstu.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import ru.spbstu.model.FileInfo;
import ru.spbstu.service.SegmentStorageService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@RestController
public class FileUploadController {

    @Autowired
    private SegmentStorageService segmentStorageService;

    @PostMapping("/api/files/uploadFile")
    public FileInfo uploadFile(@RequestParam("file")MultipartFile file) {
        segmentStorageService.save(file);
        return null; // TODO:
    }

    @PostMapping("/api/file/uploadFiles")
    public List<FileInfo> uploadFiles(@RequestParam("files") MultipartFile[] files) {
        return Arrays.stream(files)
                .toList()
                .stream()
                .map(this::uploadFile)
                .toList();
    }

}