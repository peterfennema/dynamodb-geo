package nl.peterfennema.geo;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.geo.GeoDataManager;
import com.amazonaws.geo.GeoDataManagerConfiguration;
import com.amazonaws.geo.model.GeoPoint;
import com.amazonaws.geo.model.PutPointRequest;
import com.amazonaws.geo.model.QueryRadiusRequest;
import com.amazonaws.geo.model.QueryRadiusResult;
import com.amazonaws.geo.util.GeoTableUtil;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;

/**
 * Integration tests for the dynamodb-geo library. 
 * These tests require a locally running server on DYNAMODB_ENDPOINT (defaults to  http://localhost:8000).
 * @see <a href="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html>Running DynamoDB on Your Computer</a>.
 * @author Peter Fennema
 */
public class SimpleGeoClientTest {
	
	static String TABLENAME = "geo-test";
	static String DYNAMODB_ENDPOINT = "http://localhost:8000";
	
	GeoDataManager geoDataManager;

	@Before
	public void setUp() throws Exception {
		// Fake credentials for local testing (are ignored by the test server)
		AWSCredentials credentials = new BasicAWSCredentials("MY_ACCESS_KEY", "MY_SECRET_KEY");
        AmazonDynamoDBClient ddbClient = new AmazonDynamoDBClient(credentials);
        ddbClient.setEndpoint(DYNAMODB_ENDPOINT);
        
        // Delete the table for every new test run, so we start from scratch
        if (ddbClient.listTables().getTableNames().contains(TABLENAME)) {
        	ddbClient.deleteTable(new DeleteTableRequest(TABLENAME));
        }
         
        // Create a table with the primary key and range key with dynamodb-geo defaults
        GeoDataManagerConfiguration geoConfig = new GeoDataManagerConfiguration(ddbClient, TABLENAME);
        CreateTableRequest createTableRequest = GeoTableUtil.getCreateTableRequest(geoConfig);
        ddbClient.createTable(createTableRequest);

        geoDataManager = new GeoDataManager(geoConfig);
	}

	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Puts a point and executes a query to retrieve the point.
	 */
	@Test
	public void testThatPointIsPut() {

		// Add a point to the database
        GeoPoint geoPoint = new GeoPoint(47.61121, -122.31846);
        AttributeValue rangeKeyValue = new AttributeValue().withS("someRangeKey");
        PutPointRequest putPointRequest = new PutPointRequest(geoPoint, rangeKeyValue);
        geoDataManager.putPoint(putPointRequest);

        // Query for all points within 5000 meter of the point we added (which should result in 1 point)
        GeoPoint currentPoint = geoPoint;
        List<String> attributesToGet = new ArrayList<String>();
        attributesToGet.add("rangeKey");
        attributesToGet.add("geoJson");
        attributesToGet.add("geohash");
        QueryRadiusRequest request = new QueryRadiusRequest(currentPoint, 5000);
        QueryRadiusResult geoQueryResult = geoDataManager.queryRadius(request);
        
        // Assert the result
        List<Map<String, AttributeValue>> geoItems = geoQueryResult.getItem();
        assertEquals("Query must result in 1 point", 1, geoItems.size());
        Map<String, AttributeValue> geoItem = geoItems.get(0);
        assertEquals("someRangeKey", geoItem.get("rangeKey").getS());
        assertNotNull(geoItem.get("geoJson"));
        assertNotNull(geoItem.get("geohash"));

        System.out.println(geoItems.toString());
		
	}

}
