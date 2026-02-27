package uk.co.smithdmy;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.bind.annotation.RequestMethod;

@ToString
public class PostgrestWrapper
{
    //--------------------------------------------------------------
    
    private Log log = LogFactory.getLog (this.getClass ());
    
    //--------------------------------------------------------------
    
    private record RequestMethodProperties
        (   boolean         bodyExpected,
            boolean         countExpected,
            String          responseReturn,
            String          profileType
        )
    {
        
    };
    
    //--------------------------------------------------------------
    
    @Getter
    @Setter
    private String baseUrl;
    
    private HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder ();
    
    @Getter
    @Setter
    private String resource;
    
    @Getter
    @Setter
    private String resourceId;
    
    @Getter
    @Setter
    private RequestMethod requestMethod = RequestMethod.GET;
    
    @Getter
    @Setter
    private String queryString;
    
    @Getter
    @Setter
    private Map<String, String> headers = new HashMap<> ();
    
    @Getter
    @Setter
    private JSONObject body;
    
    @Getter
    @Setter
    private String dbSchema = "public";
    
    @Getter
    @Setter
    private String jwtAuthString;
    
    @Getter
    @Setter
    private boolean commitTransaction = true;
    
    @Getter
    private final Map<RequestMethod, RequestMethodProperties> REQUEST_METHODS =
        Map.of
            (   RequestMethod.DELETE,   new RequestMethodProperties
                                            (   false,
                                                false,
                                                "minimal",
                                                "Content"
                                            ),
                RequestMethod.GET,      new RequestMethodProperties
                                            (   false,
                                                true,
                                                "minimal",
                                                "Accept"
                                            ),
                RequestMethod.HEAD,     new RequestMethodProperties
                                            (   false,
                                                false,
                                                "minimal",
                                                "Accept"
                                            ),
                RequestMethod.OPTIONS,  new RequestMethodProperties
                                            (   false,
                                                false,
                                                "minimal",
                                                "Content"
                                            ),
                RequestMethod.PATCH,    new RequestMethodProperties
                                            (   true,
                                                false,
                                                "representation",
                                                "Content"
                                            ),
                RequestMethod.POST,     new RequestMethodProperties
                                            (   true,
                                                false,
                                                "representation",
                                                "Content"
                                            ),
                RequestMethod.PUT,      new RequestMethodProperties
                                            (   true,
                                                false,
                                                "minimal",
                                                "Content"
                                            ),
                RequestMethod.TRACE,    new RequestMethodProperties
                                            (   false,
                                                false,
                                                "minimal",
                                                "Content"
                                            )
            );
    
    //--------------------------------------------------------------
    
    /**
      * Takes the Content-Range from the PostgREST response header and returns the rows returned in the
      * response body (in the context of the wider table).
      * 
      * @param  contentRange  The Content-Range response header
      */
    private Map<String, Integer> parseContentRange (String contentRange)
    {
        log.info ("Parsing Content-Range header: " + contentRange);
        
        // Match defined content range, e.g. 1-10/15, 11-20/*
        
        Pattern pattern = Pattern.compile ("(\\d+)-(\\d+)/(\\d+|\\*)");
        Matcher matcher = pattern.matcher (contentRange);

        if (matcher.matches ())
        {
            // record number = record index + 1
            int recordFrom = Integer.parseInt (matcher.group (1)) + 1;
            int recordTo = Integer.parseInt (matcher.group (2)) + 1;
            
            String recordCountValue = matcher.group (3);
            int recordCount =
                (  recordCountValue.equals ("*")
                    ?  0
                    :  Integer.parseInt (recordCountValue)
                );
            
            Map<String, Integer> returnMap = Map.of
                (   "record_from",   recordFrom,
                    "record_to",     recordTo,
                    "record_count",  recordCount
                );
            
            return returnMap;
        }
        
        throw new IllegalArgumentException ("Invalid Content-Range format: " + contentRange);
    }
    
    //--------------------------------------------------------------
    
    private JSONObject getInternalServerErrorResponse (Throwable exception)
    {
        JSONObject apiResponse = new JSONObject ();
        
        apiResponse = apiResponse.put ("status_code", HttpURLConnection.HTTP_INTERNAL_ERROR);
        
        log.warn (exception.getMessage ());
        
        return apiResponse;
    }
    
    //--------------------------------------------------------------
    
    private void buildHttpRequest ()
    {
        var queryString =
            (   this.queryString == null
                ?   ""
                :   !this.queryString.startsWith ("?")
                    ?   "?" + this.queryString
                    :   this.queryString
            );
        
        this.httpRequestBuilder =
            this.httpRequestBuilder
                .uri
                    (   URI
                            .create
                                (   String
                                        .format
                                            (   this.baseUrl + "%s%s",
                                                this.resource + (this.resource.endsWith ("/") ? "" : "/"),
                                                queryString
                                            )
                                )
                    );
        
        this.httpRequestBuilder =
            this.httpRequestBuilder
                .method
                    (   this.requestMethod.toString (),
                        REQUEST_METHODS
                            .get (this.requestMethod)
                            .bodyExpected
                                ?   HttpRequest.BodyPublishers.ofString (this.body.toString ())
                                :   HttpRequest.BodyPublishers.noBody ()
                    );
        
        this.httpRequestBuilder =
            this.httpRequestBuilder
                .header
                    (   "Content-Type",
                        "application/json"
                    );
        
        this.httpRequestBuilder =
            this.httpRequestBuilder
                .header
                    (   "Prefer",
                            "count=exact" // Returns exact count of returned resources
                        +   ", return="
                        +   REQUEST_METHODS
                                .get (this.requestMethod)
                                .responseReturn ()
                        +   ", tx=" // For determining whether to commit a write request
                        +   (  this.commitTransaction
                                ?  "commit"
                                :  "rollback"
                            )
                    );
        
        this.httpRequestBuilder =
            this.httpRequestBuilder
                .header
                    (       REQUEST_METHODS
                                .get (this.requestMethod)
                                .profileType ()
                        +   "-Profile",
                        this.dbSchema
                    );
        
        if (this.jwtAuthString != null)
        {
            this.httpRequestBuilder =
                this.httpRequestBuilder
                    .header ("Authorization", "Bearer " + this.jwtAuthString);
        };
        
        for (Map.Entry<String, String> header : this.headers.entrySet ())
        {
            this.httpRequestBuilder =
                this.httpRequestBuilder
                    .header
                        (   header.getKey (),
                            header.getValue ()
                        );
        }
    }
    
    //--------------------------------------------------------------
    
    public JSONObject getHttpResponse ()
    {
        JSONObject apiResponse = new JSONObject ();
        
        HttpClient httpClient =
            SingleHttpClient
                .getInstance ()
                .getHttpClient ();
        
        this.buildHttpRequest ();
        var httpRequest = this.httpRequestBuilder.build ();
        
        log.info (httpRequest.headers ().toString ());
        
        // Send Asynchronous HTTP request
        CompletableFuture<HttpResponse<String>> futureResponse =
            httpClient
                .sendAsync
                    (   httpRequest,
                        HttpResponse
                            .BodyHandlers
                            .ofString ()
                    );
        
        // Process the HTTP response as it is received
        futureResponse.thenApply (HttpResponse::body);
        
        // Get the HTTP response
        HttpResponse<String> httpResponse;
        try
        {
            httpResponse = futureResponse.get ();
        }
        catch (InterruptedException | ExecutionException exception)
        {
            return this.getInternalServerErrorResponse (exception);
        }
        
        log.info (httpResponse.toString ());
        
        apiResponse = apiResponse.put ("status_code", httpResponse.statusCode ());
        
        HttpStatusCode httpStatusCode = HttpStatusCode.valueOf (httpResponse.statusCode ());
        
        if (!httpStatusCode.isError ())
        {
            // Create the appropriate JSON Array OR Object depending on the type returned by the API response
            if (httpResponse.body ().startsWith ("["))
            {
                apiResponse = apiResponse.put ("body", new JSONArray (httpResponse.body ()));
            }
            else if (httpResponse.body ().startsWith ("{"))
            {
                apiResponse = apiResponse.put ("body", new JSONObject (httpResponse.body ()));
            }
            
            // Get row details for GET requests
            if (this.requestMethod == RequestMethod.GET)
            {
                // Get the Content-Range header in the response
                String contentRange =
                    httpResponse
                        .headers ()
                        .firstValue ("Content-Range")
                        .orElse ("");
                
                switch (apiResponse.getJSONArray ("body").length ())
                {
                    case 0:
                    {
                        apiResponse = apiResponse.put ("rownum_from", 0);
                        apiResponse = apiResponse.put ("rownum_to", 0);
                        apiResponse = apiResponse.put ("rowcount", 0);
                        break;
                    }
                    default:
                    {
                        for (String contentRangeKey : this.parseContentRange (contentRange).keySet ())
                        {
                            apiResponse =
                                apiResponse
                                    .put
                                        (   contentRangeKey,
                                            this
                                                .parseContentRange (contentRange)
                                                .get (contentRangeKey)
                                        );
                        }
                    }
                }
            }
            
            // Get the Content-Location header in the response
            String contentLocation;
            try
            {
                contentLocation =
                    URLDecoder.decode
                        (   httpResponse
                                .headers ()
                                .firstValue ("Content-Location")
                                .orElse (""),
                            StandardCharsets
                                .UTF_8
                                .toString ()
                        );
            }
            catch (UnsupportedEncodingException exception) // This *should* never happen unless UTF-8 encoding ceases to be ...
            {
                throw new AssertionError ("UTF-8 is unknown");
            }
            
            apiResponse = apiResponse.put ("content_location", contentLocation);
        }
        else
        {
            apiResponse = apiResponse.put ("body", new JSONObject (httpResponse.body ()));
        }
        
        log.info ("postgREST database API response: " + apiResponse.toString ());
        
        return apiResponse;
    }
    
    //--------------------------------------------------------------
    
    // CONSTRUCTOR
    public PostgrestWrapper
        (   String baseUrl,
            String resource
        )
    {
        this.baseUrl = baseUrl;
        this.resource = resource;
    }
    
    //--------------------------------------------------------------
    
    public static JSONObject authenticateUser
        (   String  baseUrl,
            String  functionName,
            String  email,
            String  password
        )
    {
        final int FAILURE_DELAY_SECONDS = 1;
        
        var authPostgrestWrapper =
            new PostgrestWrapperBuilder (baseUrl, functionName)
                .requestMethod (RequestMethod.POST)
                .body
                    (   new JSONObject ()
                            .put ("p_email", email)
                            .put ("p_password", password)
                    )
                .build ();
        
        //System.out.println (authPostgrestWrapper.toString ());
        
        var authHttpResponse = authPostgrestWrapper.getHttpResponse ();
        
        switch (authHttpResponse.getInt ("status_code"))
        {
            case 200:
                authHttpResponse.put ("succeeded", true);
                break;
            case 403:
                authHttpResponse.put ("succeeded", false);
                
                try
                {
                    TimeUnit.SECONDS.sleep (FAILURE_DELAY_SECONDS); // Built in delay to mitigate DDoS attacks
                }
                catch (InterruptedException exception)
                {
                    throw new UnsupportedOperationException ("Unsupported API response: " + authHttpResponse.getInt ("status_code"));
                }
                
                break;
            default:
                throw new UnsupportedOperationException ("Unsupported API response: " + authHttpResponse.getInt ("status_code"));
        }
        
        return authHttpResponse;
    }
    
    //--------------------------------------------------------------
}
