package uk.co.smithdmy;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.bind.annotation.RequestMethod;

@SpringBootTest(classes = TestConfig.class)
@TestInstance(Lifecycle.PER_CLASS)
public class PostgrestWrapperTest
{
    private PostgrestStub postgrestStub;
    private String authToken;
    
    private void setToken ()
    {
        if (this.authToken == null)
        {
            var authResponse =
                PostgrestWrapper.authenticateUser
                    (   "http://localhost:8001/",
                        "rpc/f_login",
                        "user@email.com",
                        "password"
                    );
            
            if (!authResponse.getBoolean ("succeeded"))
            {
                throw new IllegalAccessError ();
            }
            
            this.authToken =
                authResponse
                    .getJSONObject ("body")
                    .getString ("authToken");
        }
    }
    
    @BeforeAll
    void testsSetup () throws IOException, SQLException, ClassNotFoundException
    {
        postgrestStub = new PostgrestStub ();
        
        postgrestStub.startServer ();
    }
    
    @Test
    void testUnauthenticatedResponse ()
    {
        var postgrestWrapper =
            new PostgrestWrapperBuilder ("http://localhost:8001/", "movies")
                .build ();
        
        var httpResponse = postgrestWrapper.getHttpResponse ();
        
        Assertions.assertEquals (401, httpResponse.getInt ("status_code"));
        
        Assertions.assertEquals
            (   "Null/incorrect authentication key supplied. Access denied.",
                httpResponse
                    .getJSONObject ("body")
                    .getString ("message")
            );
    }
    
    @Test
    void testBasicResponse ()
    {
        this.setToken ();
        
        var postgrestWrapper =
            new PostgrestWrapperBuilder ("http://localhost:8001/", "movies")
                .header (Map.entry ("Authorization", "Bearer " + this.authToken))
                .build ();
        
        JSONObject httpResponse;
        
        try
        {
            httpResponse = postgrestWrapper.getHttpResponse ();
        }
        catch (JSONException exception)
        {
            Assertions.fail ("Failed to construct JSON object from API response.");
            return;
        }
        
        Assertions.assertEquals (200, httpResponse.getInt ("status_code"));
        
        try
        {
            @SuppressWarnings ("unused")
            var responseBody = new JSONArray (httpResponse.getJSONArray ("body"));
        }
        catch (JSONException exception)
        {
            Assertions.fail ("Failed to get JSON array of response body.");
            return;
        }
    }
    
    @Test
    void testEquality ()
    {
        this.setToken ();
        
        var expectedResponseBody = new JSONArray ("[{\"id\":60,\"runningMins\":101,\"title\":\"Groundhog Day\"}]");
        
        var postgrestWrapper =
            new PostgrestWrapperBuilder ("http://localhost:8001/", "movies")
                .header (Map.entry ("Authorization", "Bearer " + this.authToken))
                .queryString ("id=eq.60")
                .build ();
        
        var httpResponse = postgrestWrapper.getHttpResponse ();
        
        Assertions.assertEquals (200, httpResponse.getInt ("status_code"));
        
        Assertions.assertTrue (expectedResponseBody.similar (httpResponse.get ("body")));
        
        postgrestWrapper =
            new PostgrestWrapperBuilder ("http://localhost:8001/", "movies")
                .header (Map.entry ("Authorization", "Bearer " + this.authToken))
                .queryString ("?id=eq.60") // Added superfluous ? to query string (normally added on)
                .build ();
        
        httpResponse = postgrestWrapper.getHttpResponse ();
        
        Assertions.assertEquals (200, httpResponse.getInt ("status_code"));
        
        Assertions.assertTrue (expectedResponseBody.similar (httpResponse.get ("body")));
    }
    
    @Test
    void testQuerying ()
    {
        this.setToken ();
        
        var expectedResponseBody = new JSONArray ("[{\"id\":80,\"runningMins\":181,\"title\":\"Avengers: Endgame\"},{\"id\":110,\"runningMins\":178,\"title\":\"Lord of the Rings: The Fellowship of the Ring, The\"},{\"id\":20,\"runningMins\":166,\"title\":\"Dune: Part Two\"}]");
        
        var postgrestWrapper =
            new PostgrestWrapperBuilder ("http://localhost:8001/", "movies")
                .header (Map.entry ("Authorization", "Bearer " + this.authToken))
                .queryString ("running_mins=gt.150&order=running_mins.desc")
                .build ();
        
        var httpResponse = postgrestWrapper.getHttpResponse ();
        
        Assertions.assertEquals (200, httpResponse.getInt ("status_code"));
        
        Assertions.assertTrue (expectedResponseBody.similar (httpResponse.get ("body")));expectedResponseBody = new JSONArray ("[{\"id\":80,\"runningMins\":181,\"title\":\"Avengers: Endgame\"},{\"id\":110,\"runningMins\":178,\"title\":\"Lord of the Rings: The Fellowship of the Ring, The\"},{\"id\":20,\"runningMins\":166,\"title\":\"Dune: Part Two\"}]");
        
        expectedResponseBody = new JSONArray ("[{\"id\":20,\"runningMins\":166,\"title\":\"Dune: Part Two\"}]");
        
        postgrestWrapper =
            new PostgrestWrapperBuilder ("http://localhost:8001/", "movies")
                .header (Map.entry ("Authorization", "Bearer " + this.authToken))
                .queryString ("id=eq.20")
                .build ();
        
        httpResponse = postgrestWrapper.getHttpResponse ();
        
        Assertions.assertEquals (200, httpResponse.getInt ("status_code"));
        
        Assertions.assertTrue (expectedResponseBody.similar (httpResponse.get ("body")));
    }
    
    @Test
    void testSorting ()
    {
        this.setToken ();
        
        var expectedResponseBody = new JSONArray ("[{\"id\":60,\"runningMins\":101,\"title\":\"Groundhog Day\"},{\"id\":130,\"runningMins\":108,\"title\":\"Back to the Future Part II\"},{\"id\":50,\"runningMins\":115,\"title\":\"Incredibles, The\"},{\"id\":150,\"runningMins\":123,\"title\":\"Amélie\"},{\"id\":30,\"runningMins\":129,\"title\":\"Twelve Monkeys\"},{\"id\":120,\"runningMins\":130,\"title\":\"Knives Out\"},{\"id\":90,\"runningMins\":136,\"title\":\"Matrix, The\"},{\"id\":10,\"runningMins\":137,\"title\":\"Terminator 2: Judgement Day\"},{\"id\":100,\"runningMins\":139,\"title\":\"Everything Everywhere All at Once\"},{\"id\":140,\"runningMins\":140,\"title\":\"Spider-Man: Across the Spider-Verse\"},{\"id\":70,\"runningMins\":142,\"title\":\"Shawshank Redemption, The\"},{\"id\":40,\"runningMins\":148,\"title\":\"Inception\"},{\"id\":20,\"runningMins\":166,\"title\":\"Dune: Part Two\"},{\"id\":110,\"runningMins\":178,\"title\":\"Lord of the Rings: The Fellowship of the Ring, The\"},{\"id\":80,\"runningMins\":181,\"title\":\"Avengers: Endgame\"}]");
        
        var postgrestWrapper =
            new PostgrestWrapperBuilder ("http://localhost:8001/", "movies")
                .header (Map.entry ("Authorization", "Bearer " + this.authToken))
                .queryString ("order=running_mins.asc")
                .build ();
        
        var httpResponse = postgrestWrapper.getHttpResponse ();
        
        Assertions.assertEquals (200, httpResponse.getInt ("status_code"));
        
        Assertions.assertTrue (expectedResponseBody.similar (httpResponse.get ("body")));
        
        expectedResponseBody = new JSONArray ("[{\"id\":80,\"runningMins\":181,\"title\":\"Avengers: Endgame\"},{\"id\":110,\"runningMins\":178,\"title\":\"Lord of the Rings: The Fellowship of the Ring, The\"},{\"id\":20,\"runningMins\":166,\"title\":\"Dune: Part Two\"}]");
        
        postgrestWrapper =
            new PostgrestWrapperBuilder ("http://localhost:8001/", "movies")
                .header (Map.entry ("Authorization", "Bearer " + this.authToken))
                .queryString ("order=running_mins.desc&running_mins=gt.150")
                .build ();
        
        httpResponse = postgrestWrapper.getHttpResponse ();
        
        Assertions.assertEquals (200, httpResponse.getInt ("status_code"));
        
        Assertions.assertTrue (expectedResponseBody.similar (httpResponse.get ("body")));
    }
    
    @Test
    void testExceptionStatusCodes ()
    {
        this.setToken ();
        
        var postgrestWrapper =
            new PostgrestWrapperBuilder ("http://localhost:8001/", "movies")
                .header (Map.entry ("Authorization", "Bearer " + this.authToken))
                .queryString ("order=running_mins.invalid")
                .build ();
        
        var httpResponse = postgrestWrapper.getHttpResponse ();
        
        Assertions.assertEquals (400, httpResponse.getInt ("status_code"));
        
        Assertions.assertEquals
            (   "Query Parameter \"order\" contains illegal direction modifier. Must be .asc, .desc, or omitted.",
                httpResponse.getJSONObject ("body").getString ("message")
            );
        
        postgrestWrapper =
            new PostgrestWrapperBuilder ("http://localhost:8001/", "movies")
                .body (new JSONObject ())
                .requestMethod (RequestMethod.POST)
                .build ();
        
        httpResponse = postgrestWrapper.getHttpResponse ();
        
        Assertions.assertEquals (405, httpResponse.getInt ("status_code"));
        
        Assertions.assertEquals
            (   "Invalid method: POST.",
                httpResponse.getJSONObject ("body").getString ("message")
            );
        
        postgrestWrapper =
            new PostgrestWrapperBuilder ("http://localhost:8001/", "movies")
                .header (Map.entry ("Authorization", "Bearer " + this.authToken))
                .queryString ("id=eq.string")
                .build ();
        
        httpResponse = postgrestWrapper.getHttpResponse ();
        
        Assertions.assertEquals (500, httpResponse.getInt ("status_code"));
        
        Assertions.assertEquals
            (   "A SQLException was thrown during querying.",
                httpResponse.getJSONObject ("body").getString ("message")
            );
    }
    
    @AfterAll
    void testsTeardown ()
    {
        postgrestStub.stopServer ();
    }
}
