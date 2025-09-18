package org.apache.coyote.http11;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Http11ProcessorNio {

    public static class Session {
        private static final int READ_BUF = 16 * 1024;
        private static final int WRITE_BUF = 16 * 1024;

        private final SocketChannel ch;
        private final ByteBuffer readBuf = ByteBuffer.allocate(READ_BUF);

        // 헤더/상태
        private boolean headersParsed = false;
        private boolean keepAlive = true;

        // 응답 헤더/바디 버퍼(간단화)
        private ByteBuffer headerBuf;
        private ByteBuffer bodyBuf;

        // /slow 같은 큰 응답을 스트리밍으로 보낼 때
        private long remainingBytes = 0;
        private final ByteBuffer zeroChunk = ByteBuffer.allocate(WRITE_BUF); // 제로로 채워진 버퍼 재사용

        public Session(SocketChannel ch) {
            this.ch = ch;
        }

        public boolean readAndParseRequest() throws IOException {
            int n = ch.read(readBuf);
            if (n == -1) {
                closeQuietly();
                return false;
            }
            readBuf.flip();
            // 아주 단순한 파싱: 헤더 끝(\r\n\r\n)까지 들어왔는지만 확인
            String s = new String(readBuf.array(), 0, readBuf.limit());
            if (s.contains("\r\n\r\n")) {
                headersParsed = true;
                // keep-alive 판단(대충)
                keepAlive = !s.toLowerCase().contains("connection: close");
            }
            readBuf.compact();
            return headersParsed;
        }

        public void handleWithController() {
            // 라우팅 매우 단순화: 첫 줄로 path만 잡자
            String req = new String(readBuf.array(), 0, readBuf.position());
            String path = req.split(" ")[1];

            if ("/slow".equals(path)) {
                // 100MB 전송을 "스트리밍"으로. 큰 배열 만들지 않음.
                remainingBytes = 100L * 1024 * 1024;

                String headers = "HTTP/1.1 200 OK\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "Content-Length: " + remainingBytes + "\r\n" +
                        (keepAlive ? "Connection: keep-alive\r\n" : "Connection: close\r\n") +
                        "\r\n";
                headerBuf = ByteBuffer.wrap(headers.getBytes());

                zeroChunk.clear(); // 0으로 채워진 상태
                bodyBuf = null;    // 바디는 chunk 반복 생성(= zeroChunk 재사용)
            } else {
                byte[] body = "OK".getBytes();
                String headers = "HTTP/1.1 200 OK\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "Content-Length: " + body.length + "\r\n" +
                        (keepAlive ? "Connection: keep-alive\r\n" : "Connection: close\r\n") +
                        "\r\n";
                headerBuf = ByteBuffer.wrap(headers.getBytes());
                bodyBuf = ByteBuffer.wrap(body);
                remainingBytes = 0;
            }
        }

        public boolean flush() throws IOException {
            // 1) 헤더 먼저
            if (headerBuf != null && headerBuf.hasRemaining()) {
                ch.write(headerBuf);
                if (headerBuf.hasRemaining()) {
                    return true; // 아직 쓸 게 남아있음
                }
                headerBuf = null;
            }

            // 2) 작은 바디면 한 번에
            if (bodyBuf != null) {
                ch.write(bodyBuf);
                if (bodyBuf.hasRemaining()) {
                    return true;
                }
                bodyBuf = null;
            }

            // 3) /slow 대용량 바디 스트리밍
            if (remainingBytes > 0) {
                zeroChunk.clear();
                int toWrite = (int) Math.min(zeroChunk.remaining(), remainingBytes);
                zeroChunk.limit(toWrite);
                int written = ch.write(zeroChunk);
                if (written > 0) {
                    remainingBytes -= written;
                }
                // 소켓이 꽉 차면 OP_WRITE 유지
                return remainingBytes > 0;
            }

            // 모두 완료
            return false;
        }

        public boolean keepAlive() {
            return keepAlive;
        }

        public void fail(Exception e) {
            try {
                byte[] body = "Internal Server Error".getBytes();
                String headers = "HTTP/1.1 500 Internal Server Error\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "Content-Length: " + body.length + "\r\n" +
                        "Connection: close\r\n\r\n";
                headerBuf = ByteBuffer.wrap(headers.getBytes());
                bodyBuf = ByteBuffer.wrap(body);
                keepAlive = false;
            } catch (Exception ignore) {
            }
        }

        public void closeQuietly() {
            try {
                ch.close();
            } catch (IOException ignore) {
            }
        }
    }
}
