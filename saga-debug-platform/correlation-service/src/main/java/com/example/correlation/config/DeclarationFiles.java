package com.example.correlation.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public final class DeclarationFiles {
    private static final Logger log = LoggerFactory.getLogger(DeclarationFiles.class);

    private DeclarationFiles() {
    }

    public static List<Resource> find(String kind, String directory) {
        List<Resource> resources = new ArrayList<>(packaged(kind));
        resources.addAll(external(kind, directory));
        return resources;
    }

    private static List<Resource> packaged(String kind) {
        try {
            return Arrays.asList(new PathMatchingResourcePatternResolver()
                    .getResources("classpath*:" + kind + "/*.yaml"));
        } catch (Exception e) {
            log.error("Failed to scan the classpath for {} declarations: {}", kind, e.getMessage(), e);
            return List.of();
        }
    }

    private static List<Resource> external(String kind, String directory) {
        if (directory == null || directory.isBlank()) {
            return List.of();
        }

        File folder = new File(directory, kind);
        if (!folder.isDirectory()) {
            log.warn("correlation.declarations.dir is set but {} is not a directory; "
                    + "no external {} declarations were read", folder.getAbsolutePath(), kind);
            return List.of();
        }

        File[] files = folder.listFiles((dir, fileName) ->
                fileName.endsWith(".yaml") || fileName.endsWith(".yml"));
        if (files == null || files.length == 0) {
            return List.of();
        }

        return Arrays.stream(files)
                .sorted(Comparator.comparing(File::getName))
                .map(file -> (Resource) new FileSystemResource(file))
                .toList();
    }
}
