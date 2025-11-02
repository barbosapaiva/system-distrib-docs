package io.sd.brain.rest;

import io.sd.brain.ipfs.IpfsClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Map;

@RestController
public class DocumentController {

    private final IpfsClient ipfs;

    public DocumentController(@Value("${ipfs.api}") String apiUrl) {
        this.ipfs = new IpfsClient(apiUrl);
    }

    @PostMapping(value = "/documents", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> upload(@RequestPart("file") MultipartFile file) throws IOException {
        String cid = ipfs.add(file.getOriginalFilename(), file.getBytes(), true);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("cid", cid, "name", file.getOriginalFilename()));
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("leader", true);
    }
}