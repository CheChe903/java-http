package org.apache.catalina.connector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.coyote.http11.Http11ProcessorNio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectorNio implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ConnectorNio.class);

    private static final int DEFAULT_PORT = 8080;
    private static final int DEFAULT_ACCEPT_COUNT = 100;
    private static final int DEFAULT_MAX_WORKERS = 200;
    private static final int WORK_QUEUE_CAPACITY = 1000;
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

    private final int port;
    private final Selector selector;
    private final ServerSocketChannel server;
    private final ExecutorService workerPool;   // 블로킹 비즈니스 전용
    private volatile boolean stopped = false;
    private Thread ioThread;

    public ConnectorNio() {
        this(DEFAULT_PORT, DEFAULT_ACCEPT_COUNT, DEFAULT_MAX_WORKERS);
    }

    public ConnectorNio(int port, int acceptCount, int maxWorkers) {
        try {
            this.port = checkPort(port);
            this.selector = Selector.open();

            // Server channel
            this.server = ServerSocketChannel.open();
            server.configureBlocking(false);
            server.bind(new InetSocketAddress(this.port), Math.max(acceptCount, DEFAULT_ACCEPT_COUNT));
            server.register(selector, SelectionKey.OP_ACCEPT);

            // 워커풀: I/O 스레드가 비즈니스 대신 실행하지 않도록 AbortPolicy
            this.workerPool = new ThreadPoolExecutor(
                    maxWorkers, maxWorkers, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(WORK_QUEUE_CAPACITY),
                    namedFactory("biz-worker-"),
                    new ThreadPoolExecutor.AbortPolicy() // 큐/스레드 풀 꽉 차면 즉시 예외 → 503 같은 빠른 실패로 처리 권장
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize ConnectorNio", e);
        }
    }

    public void start() {
        stopped = false;
        ioThread = new Thread(this, "nio-io-thread");
        ioThread.setDaemon(true);
        ioThread.start();
        log.info("WAS(NIO) started on port {}", port);
    }

    @Override
    public void run() {
        try {
            while (!stopped) {
                selector.select(); // wakeup()으로 깨울 수 있음
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    // 동시 이벤트를 모두 처리: write 우선 → 지연 축소에 유리
                    if (key.isAcceptable()) {
                        onAccept(key);
                        // onAccept 내에서 register(OP_READ) 하므로 다음 분기도 가능
                    }
                    if (key.isValid() && key.isWritable()) {
                        onWrite(key);
                    }
                    if (key.isValid() && key.isReadable()) {
                        onRead(key);
                    }
                }
            }
        } catch (IOException e) {
            if (!stopped) {
                log.error("NIO loop error", e);
            }
        } finally {
            safeClose(selector);
            safeClose(server);
        }
    }

    private void onAccept(SelectionKey key) throws IOException {
        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
        SocketChannel ch = ssc.accept();
        if (ch == null) {
            return;
        }

        ch.configureBlocking(false);
        ch.socket().setTcpNoDelay(true);
        // 필요시 타임아웃/버퍼 옵션 조정 가능 (SO_SNDBUF 과대 설정은 역효과 가능)

        log.info("connect host: {}, port: {}", ch.socket().getInetAddress(), ch.socket().getPort());

        // 세션(읽기/쓰기 버퍼 + 상태) 부착
        Http11ProcessorNio.Session session = new Http11ProcessorNio.Session(ch);
        ch.register(selector, SelectionKey.OP_READ, session);
    }

    private void onRead(SelectionKey key) {
        Http11ProcessorNio.Session session = (Http11ProcessorNio.Session) key.attachment();
        try {
            // 1) 요청 헤더/바디 파싱까지는 논블록 IO로 처리
            boolean requestReady = session.readAndParseRequest();
            if (!requestReady) {
                // 더 읽을 게 남아있거나 아직 파싱 미완 → 다음 READ 이벤트 기다림
                return;
            }

            // 2) 블로킹 가능 컨트롤러 실행은 워커풀로 던짐 (I/O 스레드는 절대 실행 금지)
            try {
                workerPool.execute(() -> {
                    try {
                        session.handleWithController(); // 내부에서 SlowController 가능
                    } catch (Exception e) {
                        session.fail(e);
                    } finally {
                        // 응답 준비가 끝났으면 쓰기 이벤트 등록
                        enableWrite(key);
                    }
                });
            } catch (RejectedExecutionException rex) {
                // 워커가 꽉 찬 경우: 빠른 실패 처리 (여기서는 503 준비 → write 등록)
                enableWrite(key);
            }

            // 요청 파이프라이닝/바디 정책에 따라 READ 유지/해제 선택
            // 필요하면 disableRead(key); 로 일시 해제 가능

        } catch (IOException e) {
            session.closeQuietly();
            key.cancel();
        }
    }

    private void onWrite(SelectionKey key) {
        Http11ProcessorNio.Session session = (Http11ProcessorNio.Session) key.attachment();
        try {
            // flush()는 "아직 보낼 데이터가 남아있으면 true"를 반환하도록 가정
            // (남음=true → OP_WRITE 유지, 완료=false → OP_WRITE 해제)
            boolean hasPending = session.flush(); // 가능한 만큼 전송 (부분 쓰기 처리)
            if (hasPending) {
                // 남은 데이터가 있으므로 WRITE 관심 유지 (다음 write-ready에 재개)
                enableWrite(key); // idempotent
            } else {
                // 전송 완료 → WRITE 관심 해제
                disableWrite(key);

                if (session.keepAlive()) {
                    enableReadOnly(key); // 다음 요청 대비
                } else {
                    session.closeQuietly();
                    key.cancel();
                }
            }
        } catch (IOException e) {
            session.closeQuietly();
            key.cancel();
        }
    }

    /* -------- interestOps helpers (selector 스레드 외에서도 호출 가능하도록 wakeup 사용) -------- */

    private void enableWrite(SelectionKey key) {
        synchronized (key) {
            if (!key.isValid()) {
                return;
            }
            int ops = key.interestOps();
            if ((ops & SelectionKey.OP_WRITE) == 0) {
                key.interestOps(ops | SelectionKey.OP_WRITE);
            }
        }
        selector.wakeup();
    }

    private void disableWrite(SelectionKey key) {
        synchronized (key) {
            if (!key.isValid()) {
                return;
            }
            int ops = key.interestOps();
            if ((ops & SelectionKey.OP_WRITE) != 0) {
                key.interestOps(ops & ~SelectionKey.OP_WRITE);
            }
        }
        // 굳이 wakeup 필요 없음
    }

    private void enableReadOnly(SelectionKey key) {
        synchronized (key) {
            if (!key.isValid()) {
                return;
            }
            key.interestOps(SelectionKey.OP_READ);
        }
    }

    @SuppressWarnings("unused")
    private void disableRead(SelectionKey key) {
        synchronized (key) {
            if (!key.isValid()) {
                return;
            }
            int ops = key.interestOps();
            if ((ops & SelectionKey.OP_READ) != 0) {
                key.interestOps(ops & ~SelectionKey.OP_READ);
            }
        }
    }

    public void stop() {
        stopped = true;
        selector.wakeup(); // select() 깨워서 종료 루프로
        try {
            if (ioThread != null) {
                ioThread.join(2000);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        workerPool.shutdown();
        try {
            if (!workerPool.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            workerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /* -------------------- utils -------------------- */

    private int checkPort(int port) {
        return (port < 1 || port > 65535) ? DEFAULT_PORT : port;
    }

    private static ThreadFactory namedFactory(String prefix) {
        return r -> {
            Thread t = new Thread(r);
            t.setName(prefix + t.getId());
            t.setDaemon(false);
            return t;
        };
    }

    private static void safeClose(Selector sel) {
        if (sel != null) {
            try {
                sel.close();
            } catch (IOException ignore) {
            }
        }
    }

    private static void safeClose(Channel ch) {
        if (ch != null) {
            try {
                ch.close();
            } catch (IOException ignore) {
            }
        }
    }
}
