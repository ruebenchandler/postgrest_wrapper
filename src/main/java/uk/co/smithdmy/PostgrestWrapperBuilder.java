package uk.co.smithdmy;

import java.util.Map;

import lombok.NoArgsConstructor;

import org.json.JSONObject;
import org.springframework.web.bind.annotation.RequestMethod;

@NoArgsConstructor
public class PostgrestWrapperBuilder
{
    //---------------------------------------------------------------------
    
    PostgrestWrapper postgrestWrapper;
    
    //---------------------------------------------------------------------
    
    public PostgrestWrapperBuilder resourceId (String resourceId)
    {
        postgrestWrapper.setResourceId (resourceId);
        
        return this;
    }
    
    //---------------------------------------------------------------------
    
    public PostgrestWrapperBuilder requestMethod (RequestMethod requestMethod)
    {
        postgrestWrapper.setRequestMethod (requestMethod);
        
        return this;
    }
    
    //---------------------------------------------------------------------
    
    public PostgrestWrapperBuilder queryString (String queryString)
    {
        postgrestWrapper.setQueryString (queryString);
        
        return this;
    }
    
    //---------------------------------------------------------------------
    
    public PostgrestWrapperBuilder header (Map.Entry<String, String> header)
    {
        Map<String, String> headers = postgrestWrapper.getHeaders ();
        
        headers.put (header.getKey (), header.getValue ());
        
        postgrestWrapper.setHeaders (headers);
        
        return this;
    }
    
    //---------------------------------------------------------------------
    
    public PostgrestWrapperBuilder body (JSONObject body)
    {
        postgrestWrapper.setBody (body);
        
        return this;
    }
    
    //---------------------------------------------------------------------
    
    public PostgrestWrapperBuilder dbSchema (String dbSchema)
    {
        postgrestWrapper.setDbSchema (dbSchema);
        
        return this;
    }
    
    //---------------------------------------------------------------------
    
    public PostgrestWrapperBuilder commitTransaction (boolean commitTransaction)
    {
        postgrestWrapper.setCommitTransaction (commitTransaction);
        
        return this;
    }
    
    //---------------------------------------------------------------------
    
    public PostgrestWrapperBuilder jwtAuthString (JSONObject authResponse)
    {
        if (authResponse.getBoolean ("succeeded"))
        {
            postgrestWrapper
                .setJwtAuthString
                    (   authResponse
                            .getJSONObject("body")
                            .getString ("jwt_token")
                    );
        }
        
        return this;
    }
    
    //---------------------------------------------------------------------
    
    public PostgrestWrapper build ()
    {
        return postgrestWrapper;
    }
    
    //---------------------------------------------------------------------
    
    public PostgrestWrapperBuilder
        (   String baseUrl,
            String resource
        )
    {
        postgrestWrapper =
            new PostgrestWrapper
                (   baseUrl,
                    resource
                );
    }
    
    //---------------------------------------------------------------------
}
