package com.codingwallah.sqlwebhook.dto;

public class WebhookResponse {
    private String webhook;
    private String accessToken;
    
    // Getters and setters
    public String getWebhook() { return webhook; }
    public void setWebhook(String webhook) { this.webhook = webhook; }
    
    public String getAccessToken() { return accessToken; }
    public void setAccessToken(String accessToken) { this.accessToken = accessToken; }
}
