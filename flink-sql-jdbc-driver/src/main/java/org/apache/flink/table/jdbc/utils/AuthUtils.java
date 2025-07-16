package org.apache.flink.table.jdbc.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.awt.Desktop;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class AuthUtils {
    private AuthUtils() {}

    public static String fetchAccessToken(String tokenUrl, String refreshToken) throws IOException {
        URL url = new URL(tokenUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", "Bearer " + refreshToken);
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        int status = conn.getResponseCode();
        if (status != HttpURLConnection.HTTP_OK) {
            ErrorResponse errorResponse = parseError(conn);
            String errorDetail =
                    errorResponse != null ? errorResponse.getDetail() : "Unknown error";
            if (status == HttpURLConnection.HTTP_UNAUTHORIZED) {
                throw new NotAuthorizedException(errorDetail);
            }
            throw new AuthException(errorDetail);
        }

        TokenResponse tokenResponse = parseToken(conn);
        if (tokenResponse == null) {
            throw new AuthException("Unknown error");
        }
        return tokenResponse.getToken();
    }

    public static void openLoginPage(String url) {
        if (Desktop.isDesktopSupported()) {
            try {
                Desktop.getDesktop().browse(new URI(url));
            } catch (URISyntaxException | IOException e) {
                throw new RuntimeException("Cannot open browser window for login: " + e.getCause());
            }
        }
    }

    private static ErrorResponse parseError(HttpURLConnection conn) {
        try (InputStream errorStream = conn.getErrorStream()) {
            if (errorStream != null) {
                ObjectMapper mapper = new ObjectMapper();
                return mapper.readValue(errorStream, ErrorResponse.class);
            }
        } catch (IOException e) {
            return null;
        }
        return null;
    }

    private static TokenResponse parseToken(HttpURLConnection conn) {
        try (InputStream in = conn.getInputStream()) {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(in, TokenResponse.class);
        } catch (IOException e) {
            return null;
        }
    }

    public static class TokenResponse {
        private String token;

        public TokenResponse() {}

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }
    }

    private static class ErrorResponse {
        private String detail;

        public ErrorResponse() {}

        public String getDetail() {
            return detail;
        }

        public void setDetail(String detail) {
            this.detail = detail;
        }
    }

    public static class AuthException extends RuntimeException {
        public AuthException(String message) {
            super(message);
        }
    }

    public static class NotAuthorizedException extends AuthException {
        public NotAuthorizedException(String message) {
            super(message);
        }
    }
}
