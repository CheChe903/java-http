package org.apache.catalina.controller;

import java.io.IOException;
import org.apache.coyote.http11.AbstractController;
import org.apache.coyote.http11.HttpRequest;
import org.apache.coyote.http11.HttpResponse;
import org.apache.coyote.http11.HttpStatus;

public class SlowController extends AbstractController {

    private static final int SIZE = 100 * 1024 * 512;

    @Override
    protected void doGet(HttpRequest req, HttpResponse res) throws IOException {
        res.setStatusCode(HttpStatus.OK);
        res.setHeader("Content-Type", "application/octet-stream");

        // 큰 바디 생성 (zero-filled)
        byte[] body = new byte[SIZE];
        res.setBody(body);
    }

    @Override
    protected void doPost(HttpRequest httpRequest, HttpResponse httpResponse) throws Exception {
    }
}
