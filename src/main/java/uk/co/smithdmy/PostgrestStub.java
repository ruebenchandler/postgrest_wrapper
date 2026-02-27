package uk.co.smithdmy;

import com.sun.net.httpserver.HttpServer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

public class PostgrestStub
{
    //--------------------------------------------------------------
    
    private Log log = LogFactory.getLog (this.getClass ());
    
    //--------------------------------------------------------------
    
    private final String EMAIL = "user@email.com";
    private final String PASSWORD = "password";
    private final String AUTH_STRING = "Pd1WuMdnkuZ4pXZZGyhCTsT0Y0K4Ql";
    
    private Connection dbConnection;
    private String whereClause;
    private String orderByClause;
    private List<String> whereClausesList;
    
    //--------------------------------------------------------------
    
    // Class to hold instances of records returned from DB
    @AllArgsConstructor
    @ToString
    private class Movie
    {
        @Getter
        @Setter
        private int id;
        
        @Getter
        @Setter
        private String title;
        
        @Getter
        @Setter
        private int runningMins;
    };
    
    //--------------------------------------------------------------
    
    @Getter
    private final int PORT = 8001;
    
    @Getter
    private HttpServer httpServer;
    
    //--------------------------------------------------------------
    
    @Getter
    @Setter
    private List<Movie> filteredMovies;
    
    //--------------------------------------------------------------
    
    // For API requests to /movies/:MOVIE_ID
    private void filterMoviesById (int id) throws SQLException
    {
        List<Movie> filteredList = new ArrayList<> ();
        
        var statement = dbConnection.prepareStatement ("SELECT * FROM movies WHERE id = ?");
        statement.setInt (1, id);
        ResultSet resultSet = statement.executeQuery ();
        
        while (resultSet.next ())
        {
            filteredList.add
                (   new Movie
                        (   resultSet.getInt ("id"),
                            resultSet.getString ("title"),
                            resultSet.getInt ("running_mins")
                        )
                );
        }
        
        this.filteredMovies = filteredList;
    }
    
    //--------------------------------------------------------------
    
    // For API requests to /movies/{query_parameter(s)}
    private void queryMovies (String queryString) throws UnsupportedEncodingException, SQLException
    {
        List<Movie> filteredList = new ArrayList<> ();
        
        var baseSQL = "SELECT * FROM movies #WHERE_CLAUSE# #ORDER_BY_CLAUSE#;";
        
        log.info ("queryMovies(): Started");
        log.info ("Splitting query string: " + queryString);
        
        var queryStrings = queryString.split ("&");
        
        // Iterate query string parameters
        if (!queryString.isEmpty ())
        {
            for (var parameter : queryStrings)
            {   
                log.info ("Parsing parameter: " + parameter);
                
                Pattern pattern = Pattern.compile ("^\\??([a-zA-z_]+)=([a-z_]+)\\.(.+)$"); // The expected format of the query string
                Matcher matcher = pattern.matcher (parameter);
                
                matcher.matches ();
                
                String column = matcher.group (1);
                String operator = matcher.group (2);
                
                String value = "";
                
                log.info ("column: " + column + "; operator: " + operator);
                
                try
                {
                    value = matcher.group (3);
                    value = URLDecoder.decode (value, StandardCharsets.UTF_8.toString ());
                }
                catch (NullPointerException exception)
                {
                    value = "";
                }
                
                log.info ("value: " + value);
                
                // ORDER BY clause - for this, operator is the column to order by, and value is the direction
                if (column.equals ("order"))
                {
                    // Sanitise user input
                    if  (   Arrays.asList ("id", "title", "running_mins").contains (operator)
                            &&
                            Arrays.asList ("asc", "desc", "").contains (value)
                        )
                    {
                        orderByClause = "ORDER BY " + operator + (value.equals ("desc") ? " DESC" : " ASC");
                        
                        log.info ("Added ORDER BY clause: " + orderByClause);
                        
                        continue;
                    }
                    
                    // Raise exception for invalid input
                    throw new IllegalArgumentException ("Query Parameter \"order\" contains illegal direction modifier. Must be .asc, .desc, or omitted.");
                }
                
                // WHERE clause statements
                if  (   Arrays.asList ("title", "id").contains (column)
                        &&
                        Arrays.asList ("eq", "neq").contains (operator)
                    )
                {
                    var sqlOperator = "";
                    switch (operator)
                    {
                        case "eq": sqlOperator = " ="; break;
                        case "neq": sqlOperator = " !="; break;
                    }
                    
                    whereClause =
                            (!whereClause.equals ("") ? " AND " : "WHERE ")
                        +   whereClause
                        +   column
                        +   sqlOperator
                        +   " ?";
                    
                    whereClausesList.add (value);
                    
                    log.info ("Added " + column + sqlOperator + " " + value + " to WHERE clause");
                    
                    continue;
                }
                
                if  (   column.equals ("running_mins")
                        &&
                        Arrays.asList ("eq", "neq", "lt", "gt", "lte", "gte").contains (operator)
                    )
                {
                    var sqlOperator = "";
                    switch (operator)
                    {
                        case "eq": sqlOperator = " ="; break;
                        case "neq": sqlOperator = " !="; break;
                        case "lt": sqlOperator = " <"; break;
                        case "gt": sqlOperator = " >"; break;
                        case "lte": sqlOperator = " <="; break;
                        case "gte": sqlOperator = " >="; break;
                    }
                    
                    whereClause =
                            (!whereClause.equals ("") ? " AND " : "WHERE ")
                        +   whereClause
                        +   column
                        +   sqlOperator
                        +   " ?";
                    
                    whereClausesList.add (value);
                    
                    log.info ("Added " + column + sqlOperator + " " + value + " to WHERE clause");
                    
                    continue;
                }
            }
        }
        
        // Complete SQL statement construction
        baseSQL = baseSQL.replace ("#WHERE_CLAUSE#", whereClause);
        baseSQL = baseSQL.replace ("#ORDER_BY_CLAUSE#", orderByClause);
        
        log.info ("Query to execute: " + baseSQL);
        
        var statement = dbConnection.prepareStatement (baseSQL);
        
        // Set WHERE clause parameters
        for (int idx = 0; idx < whereClausesList.size (); idx++)
        {
            // Attempt to cast value to an integer first, then a string
            try
            {
                statement.setInt (idx + 1, Integer.valueOf (whereClausesList.get (idx)));
            }
            catch (NumberFormatException exception)
            {
                statement.setString (idx + 1, whereClausesList.get (idx));
            }
        }
        
        // Execute SQL query
        ResultSet resultSet = statement.executeQuery ();
        
        while (resultSet.next ())
        {
            filteredList.add
                (   new Movie
                        (   resultSet.getInt ("id"),
                            resultSet.getString ("title"),
                            resultSet.getInt ("running_mins")
                        )
                );
        }
        
        this.setFilteredMovies (filteredList);
        
        log.info ("queryMovies(): Ended");
    }
    
    //--------------------------------------------------------------
    
    // For populating the Content-Range header (X-Y/Z)
    private String getMoviesCount
        (   String  whereClause,
            String  orderByClause
        ) throws SQLException
    {
        var baseSQL =
            (   """
                    SELECT
                            MIN (item) - 1
                        ||  '-'
                        ||  MAX (item) - 1
                        ||  '/'
                        ||  COUNT (item) AS result_count
                    FROM
                        (   SELECT
                                ROW_NUMBER () OVER (#ORDER_BY_CLAUSE#) AS item
                            FROM
                                movies m
                            #WHERE_CLAUSE#
                        )
                """
            );
        
        baseSQL = baseSQL.replace ("#WHERE_CLAUSE#", whereClause);
        baseSQL = baseSQL.replace ("#ORDER_BY_CLAUSE#", orderByClause);
        
        var statement = dbConnection.prepareStatement (baseSQL);
        
        // Set WHERE clause parameters
        for (int idx = 0; idx < this.whereClausesList.size (); idx++)
        {
            // Attempt to cast value to an integer first, then a string
            try
            {
                statement.setInt (idx + 1, Integer.valueOf (this.whereClausesList.get (idx)));
            }
            catch (NumberFormatException exception)
            {
                statement.setString (idx + 1, this.whereClausesList.get (idx));
            }
        }
        
        ResultSet resultSet = statement.executeQuery ();
        
        resultSet.next ();
        
        return resultSet.getString ("result_count");
    }
    
    //--------------------------------------------------------------
    
    // Converts this.filteredMovies to a JSON array - used to build response body
    private String getMoviesJSON ()
    {
        ObjectMapper objectMapper = new ObjectMapper ();
        
        ObjectWriter objectWriter = objectMapper.writer ();

        return objectWriter.writeValueAsString (this.getFilteredMovies ());
    }
    
    //--------------------------------------------------------------
    
    public void startServer ()
    {
        this.httpServer.start ();
    }
    
    //--------------------------------------------------------------
    
    public void stopServer ()
    {
        this.httpServer.stop (10);
    }
    
    //--------------------------------------------------------------
    
    public void logStackTrace (String message, StackTraceElement[] stackTrace)
    {
        String stackTraceConcat = message;
        String separator = "\n";
        
        for (var stackTraceLine : stackTrace)
        {
            stackTraceConcat += separator + stackTraceLine.toString ();
        }
        
        log.error (stackTraceConcat);
    }
    
    //--------------------------------------------------------------
    
    public PostgrestStub () throws IOException, ClassNotFoundException, SQLException
    {
        // Connect to a private, in-memory H2 database
        this.dbConnection = DriverManager.getConnection ("jdbc:h2:mem:", "sa", "");
        
        // Create the HTTP server
        this.httpServer = HttpServer.create (new InetSocketAddress ("localhost", PORT), 0);
        
        // Create the DB table for the HTTP API and populate it
        var statement = dbConnection.createStatement ();
        
        var createTableSQL =
            """
                CREATE TABLE movies
                    (   id              INT PRIMARY KEY,
                        title           VARCHAR (255),
                        running_mins    INT
                    );
            """;
        statement.executeUpdate (createTableSQL);
        
        var insertRecordsSQL =
            """
                INSERT INTO movies
                SELECT 10, 'Terminator 2: Judgement Day', 137
                UNION ALL
                SELECT 20, 'Dune: Part Two', 166
                UNION ALL
                SELECT 30, 'Twelve Monkeys', 129
                UNION ALL
                SELECT 40, 'Inception', 148
                UNION ALL
                SELECT 50, 'Incredibles, The', 115
                UNION ALL
                SELECT 60, 'Groundhog Day', 101
                UNION ALL
                SELECT 70, 'Shawshank Redemption, The', 142
                UNION ALL
                SELECT 80, 'Avengers: Endgame', 181
                UNION ALL
                SELECT 90, 'Matrix, The', 136
                UNION ALL
                SELECT 100, 'Everything Everywhere All at Once', 139
                UNION ALL
                SELECT 110, 'Lord of the Rings: The Fellowship of the Ring, The', 178
                UNION ALL
                SELECT 120, 'Knives Out', 130
                UNION ALL
                SELECT 130, 'Back to the Future Part II', 108
                UNION ALL
                SELECT 140, 'Spider-Man: Across the Spider-Verse', 140
                UNION ALL
                SELECT 150, 'Amélie', 123;
            """;
        statement.executeUpdate (insertRecordsSQL);
        
        // Create contexts
        httpServer.createContext
            (   "/rpc/f_login/",
                httpExchange    ->
                {
                    // Only accept POST requests to this context
                    if (!httpExchange.getRequestMethod ().equals ("POST"))
                    {
                        byte response[] =
                            new JSONObject ()
                                .put ("message", "Invalid method: " + httpExchange.getRequestMethod () + ".")
                                .toString ()
                                .getBytes ("UTF-8");
                        
                        httpExchange.sendResponseHeaders (405, response.length);
                        
                        OutputStream out = httpExchange.getResponseBody ();
                        
                        out.write (response);
                        out.close ();
                        
                        return;
                    }
                    
                    InputStream in = httpExchange.getRequestBody ();
                    JSONObject requestBody;
                    
                    // Validate a JSON object request body has been sent
                    try
                    {
                        requestBody =
                            new JSONObject
                                (   new String
                                        (   in.readAllBytes (),
                                            StandardCharsets.UTF_8
                                        )
                                );
                    }
                    catch (JSONException exception)
                    {
                        byte response[] =
                            new JSONObject ()
                                .put ("message", "Failed to parse request body to JSON.")
                                .toString ()
                                .getBytes ("UTF-8");
                        
                        httpExchange.sendResponseHeaders (400, response.length);
                        
                        OutputStream out = httpExchange.getResponseBody ();
                        
                        out.write (response);
                        out.close ();
                        
                        return;
                    }
                    
                    in.close ();
                    
                    // Validate matching email and password
                    String email;
                    String password;
                    
                    try
                    {
                        email = requestBody.getString ("p_email");
                        password = requestBody.getString ("p_password");
                        
                        // If email/password do not match , throw the same exception as if they were missing
                        if  (   !email.equals (this.EMAIL)
                                ||
                                !password.equals (this.PASSWORD)
                            )
                        {
                            throw new NullPointerException ();
                        }
                    }
                    catch (NullPointerException exception)
                    {
                        byte response[] =
                            new JSONObject ()
                                .put ("message", "Invalid or missing email/password details.")
                                .toString ()
                                .getBytes ("UTF-8");
                        
                        httpExchange.sendResponseHeaders (401, response.length);
                        
                        OutputStream out = httpExchange.getResponseBody ();
                        
                        out.write (response);
                        out.close ();
                        
                        return;
                    }
                    
                    byte response[] =
                        new JSONObject ()
                            .put ("authToken", this.AUTH_STRING)
                            .toString ()
                            .getBytes ("UTF-8");
                    
                    httpExchange.sendResponseHeaders (200, response.length);
                    
                    OutputStream out = httpExchange.getResponseBody ();
                    
                    out.write (response);
                    out.close ();
                }
            );
        
        httpServer.createContext
            (   "/movies/",
                httpExchange    ->
                {
                    if (!httpExchange.getRequestMethod ().equals ("GET"))
                    {
                        byte response[] =
                            new JSONObject ()
                                .put ("message", "Invalid method: " + httpExchange.getRequestMethod () + ".")
                                .toString ()
                                .getBytes ("UTF-8");
                        
                        httpExchange.sendResponseHeaders (405, response.length);
                        
                        OutputStream out = httpExchange.getResponseBody ();
                        
                        out.write (response);
                        out.close ();
                        
                        return;
                    }
                    
                    String authKey =
                        httpExchange
                            .getRequestHeaders ()
                            .getFirst ("Authorization");
                    
                    // Auth key does NOT begin with "Bearer " OR incorrect auth key supplied
                    if  (   authKey == null
                            ||
                            !authKey.startsWith ("Bearer ")
                            ||
                            !authKey
                                .replace ("Bearer ", "")
                                .equals (this.AUTH_STRING)
                        )
                    {
                        byte response[] =
                            new JSONObject ()
                                .put ("message", "Null/incorrect authentication key supplied. Access denied.")
                                .toString ()
                                .getBytes ("UTF-8");
                        
                        httpExchange.sendResponseHeaders (401, response.length);
                        
                        OutputStream out = httpExchange.getResponseBody ();
                        
                        out.write (response);
                        out.close ();
                        
                        return;
                    }
                    
                    this.whereClause = "";
                    this.orderByClause = "";
                    this.whereClausesList = new ArrayList<> ();
                    
                    var pathVariable = 
                        httpExchange
                            .getRequestURI ()
                            .toString ();
                    
                    log.info ("URI requested: " + pathVariable);
                    
                    httpExchange
                        .getResponseHeaders ()
                        .add ("Content-Type", "application/json; charset=UTF-8");
                    
                    httpExchange
                        .getResponseHeaders ()
                        .add ("Content-Location", httpExchange.getRequestURI ().toASCIIString ());
                    
                    pathVariable = pathVariable.replace ("/movies/", "");
                    
                    // Handle potential path variable
                    if  (   pathVariable.startsWith ("?") // Query string supplied
                            ||
                            pathVariable.length () == 0 // No query string supplied
                        )
                    {
                        log.info ("Query string supplied: " + (pathVariable.equals ("") ? "<<NULL>>" : pathVariable));
                        
                        try
                        {
                            this.queryMovies (pathVariable);
                        }
                        catch (SQLException exception)
                        {
                            logStackTrace (exception.getMessage (), exception.getStackTrace ());
                            
                            byte response[] =
                                new JSONObject ()
                                    .put ("message", "A SQLException was thrown during querying.")
                                    .toString ()
                                    .getBytes ("UTF-8");
                            
                            httpExchange.sendResponseHeaders (500, response.length);
                            
                            OutputStream out = httpExchange.getResponseBody ();
                            
                            out.write (response);
                            out.close ();
                            
                            return;
                        }
                        catch (IllegalArgumentException exception)
                        {
                            logStackTrace (exception.getMessage (), exception.getStackTrace ());
                            
                            byte response[] =
                                new JSONObject ()
                                    .put ("message", exception.getMessage ())
                                    .toString ()
                                    .getBytes ("UTF-8");
                            
                            httpExchange.sendResponseHeaders (400, response.length);
                            
                            OutputStream out = httpExchange.getResponseBody ();
                            
                            out.write (response);
                            out.close ();
                            
                            return;
                        }
                    }
                    else // Path variable supplied
                    {
                        log.info ("Path variable supplied: " + pathVariable);
                        
                        try
                        {
                            int id = Integer.valueOf (pathVariable);
                            
                            this.filterMoviesById (id);
                        }
                        catch (SQLException exception)
                        {
                            logStackTrace (exception.getMessage (), exception.getStackTrace ());
                            
                            byte response[] =
                                new JSONObject ()
                                    .put ("message", "A SQLException was thrown during querying.")
                                    .toString ()
                                    .getBytes ("UTF-8");
                            
                            httpExchange.sendResponseHeaders (500, response.length);
                            
                            OutputStream out = httpExchange.getResponseBody ();
                            
                            out.write (response);
                            out.close ();
                            
                            return;
                        }
                    }
                    
                    byte response[] =
                        this.getMoviesJSON ()
                            .getBytes ("UTF-8");
                    
                    try
                    {
                        httpExchange
                            .getResponseHeaders ()
                            .add ("Content-Range", this.getMoviesCount (this.whereClause, this.orderByClause));
                    }
                    catch (SQLException exception)
                    {
                        logStackTrace (exception.getMessage (), exception.getStackTrace ());
                        
                        byte exceptionResponse[] =
                            new JSONObject ()
                                .put ("message", "A SQLException was thrown during querying.")
                                .toString ()
                                .getBytes ("UTF-8");
                        
                        httpExchange.sendResponseHeaders (500, exceptionResponse.length);
                        
                        OutputStream out = httpExchange.getResponseBody ();
                        
                        out.write (exceptionResponse);
                        out.close ();
                        
                        return;
                    }
                    
                    httpExchange
                        .getResponseHeaders ()
                        .add ("Range-Units", "items");
                    
                    httpExchange.sendResponseHeaders (200, response.length);
                    
                    OutputStream out = httpExchange.getResponseBody ();
                    out.write (response);
                    out.close ();
                }
            );
    }
    
    //--------------------------------------------------------------
}
