package io.bhex.broker.quote.util;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 解压缩字符串工具类
 *
 * @author boyce
 * @date 2017-06-06
 * @since 0.0.1
 **/
@Slf4j
public class ZipUtil {


    public static byte[] deflaterZip(String message) {
        Deflater deflater = new Deflater(5, true);
        try (ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream()) {
            deflater.setInput(message.getBytes());
            deflater.finish();
            byte[] buffer = new byte[1024];
            while (!deflater.finished()) {
                // returns the generated code... index
                int count = deflater.deflate(buffer);
                arrayOutputStream.write(buffer, 0, count);
            }
            arrayOutputStream.close();
            return arrayOutputStream.toByteArray();
        } catch (Exception e) {
            log.error("DEFLATER error: ", e);
        } finally {
            deflater.end();
        }
        return null;
    }

    public static byte[] gzip(String message) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(message.getBytes());
        } catch (Exception e) {
            log.error("gzip ex:", e);
        }
        return out.toByteArray();
    }

    public static String xgzip(byte[] bytes) {

        if (bytes == null || bytes.length == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        try {
            GZIPInputStream ungzip = new GZIPInputStream(in);
            byte[] buffer = new byte[256];
            int n;
            while ((n = ungzip.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new String(out.toByteArray());
    }

}