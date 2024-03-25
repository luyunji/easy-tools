package org.luyunji.tools.download.spring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 主要用于大文件下载
 */
public class FileDownload {

    private String tmpDir = "/tmp";

    /**
     * NIO读取文件到输出流
     *
     * @param filePath
     * @param outputStream
     * @throws IOException
     */
    public void FileDownload(String filePath, OutputStream outputStream) throws IOException {
        Files.copy(Paths.get(filePath), outputStream);
    }

}
