package uk.co.smithdmy;

import java.net.http.HttpClient;

public class SingleHttpClient
{
    private static SingleHttpClient instance;
    
    private HttpClient httpClient;
    
    public HttpClient getHttpClient () { return this.httpClient; }
    
    private SingleHttpClient () // Hides the constructor
    {
        this.httpClient = HttpClient.newHttpClient ();
    }
    
    public static synchronized SingleHttpClient getInstance ()
    {
        if (instance == null)
        {
            instance = new SingleHttpClient ();
        }
        
        return instance;
    }
}
