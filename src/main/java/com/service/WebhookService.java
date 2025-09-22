package com.example.sqlwebhook.service;

import com.example.sqlwebhook.dto.SolutionRequest;
import com.example.sqlwebhook.dto.WebhookRequest;
import com.example.sqlwebhook.dto.WebhookResponse;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
public class WebhookService implements CommandLineRunner {
    
    private final WebClient webClient;
    
    // SQL Query for Question 2
    private static final String SQL_QUERY = """
        SELECT 
            e1.EMP_ID,
            e1.FIRST_NAME,
            e1.LAST_NAME,
            d.DEPARTMENT_NAME,
            COUNT(e2.EMP_ID) as YOUNGER_EMPLOYEES_COUNT
        FROM 
            EMPLOYEE e1
            INNER JOIN DEPARTMENT d ON e1.DEPARTMENT = d.DEPARTMENT_ID
            LEFT JOIN EMPLOYEE e2 ON e1.DEPARTMENT = e2.DEPARTMENT 
                                  AND e2.DOB > e1.DOB
        GROUP BY 
            e1.EMP_ID, e1.FIRST_NAME, e1.LAST_NAME, d.DEPARTMENT_NAME
        ORDER BY 
            e1.EMP_ID DESC;
        """;
    
    public WebhookService() {
        this.webClient = WebClient.builder()
                .baseUrl("https://bfhldevapigw.healthrx.co.in")
                .build();
    }
    
    @Override
    public void run(String... args) throws Exception {
        System.out.println("Starting webhook workflow...");
        executeWorkflow();
    }
    
    private void executeWorkflow() {
        try {
            // Step 1: Generate webhook
            WebhookRequest request = new WebhookRequest("Utkarsh Gupta", "0101CS221146", "utkarshg9981@gmail.com");
            
            WebhookResponse response = generateWebhook(request);
            System.out.println("Received webhook: " + response.getWebhook());
            System.out.println("Received token: " + response.getAccessToken());
            
            // Step 2: Submit solution
            submitSolution(response.getAccessToken(), SQL_QUERY);
            System.out.println("Solution submitted successfully!");
            
        } catch (Exception e) {
            System.err.println("Error in workflow: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private WebhookResponse generateWebhook(WebhookRequest request) {
        return webClient.post()
                .uri("/hiring/generateWebhook/JAVA")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(Mono.just(request), WebhookRequest.class)
                .retrieve()
                .bodyToMono(WebhookResponse.class)
                .block();
    }
    
    private void submitSolution(String accessToken, String query) {
        SolutionRequest solutionRequest = new SolutionRequest(query);
        
        String response = webClient.post()
                .uri("/hiring/testWebhook/JAVA")
                .header(HttpHeaders.AUTHORIZATION, accessToken)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(Mono.just(solutionRequest), SolutionRequest.class)
                .retrieve()
                .bodyToMono(String.class)
                .block();
        
        System.out.println("Submission response: " + response);
    }
}