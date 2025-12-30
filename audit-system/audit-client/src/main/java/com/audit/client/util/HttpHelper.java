package com.audit.client.util;

import org.springframework.web.client.RestTemplate;

import java.util.Random;

public class HttpHelper {

    // 使用随机值默认日志的发送异常
    public static boolean sendGetRequest() {
        Random random = new Random();
        String url = "http://localhost:8080/v1/log/";
        url = url + random.nextInt(4, 10);

        RestTemplate restTemplate = new RestTemplate();
        try {
            String response = restTemplate.getForObject(url, String.class);
            return response != null && response.equals("success");
        } catch (Exception exception) {
            return false;
        }
    }
}
