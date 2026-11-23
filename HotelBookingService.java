// Hotel booking service: AWS Lambda + Azure Functions (Java test file for CloudCostGuard).
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.amazonaws.services.lambda.runtime.Context;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.sqs.SqsClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;

public class HotelBookingService {
    private static final String BUCKET = "hotel-invoices";
    private static final String TABLE = "bookings";
    private static final String QUEUE_URL = System.getenv("CONFIRM_QUEUE_URL");

    // AWS Lambda entry point: save invoices, confirm bookings, count archived invoices.
    public int handleRequest(Map<String, List<String>> event, Context context) {
        S3Client s3 = S3Client.create();
        SqsClient sqs = SqsClient.create();
        DynamoDbClient ddb = DynamoDbClient.create();

        List<String> bookingIds = event.get("bookingIds");

        // Load every booking one by one.
        List<Map<String, AttributeValue>> bookings = new ArrayList<>();
        for (String id : bookingIds) {
            bookings.add(ddb.getItem(b -> b.tableName(TABLE).key(Map.of("id", AttributeValue.fromS(id)))).item());
        }

        // Upload one invoice per booking.
        for (String id : bookingIds) {
            s3.putObject(b -> b.bucket(BUCKET).key("invoices/" + id + ".pdf"), RequestBody.fromString("invoice " + id));
        }

        // Send one confirmation message per booking.
        bookingIds.forEach(id -> sqs.sendMessage(b -> b.queueUrl(QUEUE_URL).messageBody(id)));

        // Count archived invoices.
        ListObjectsV2Response listing = s3.listObjectsV2(b -> b.bucket(BUCKET).prefix("invoices/"));
        return listing.keyCount();
    }

    // Find all cancelled bookings.
    public List<Map<String, AttributeValue>> cancelledBookings(DynamoDbClient ddb) {
        ScanResponse resp = ddb.scan(b -> b.tableName(TABLE));
        List<Map<String, AttributeValue>> out = new ArrayList<>();
        for (Map<String, AttributeValue> item : resp.items()) {
            if ("cancelled".equals(item.get("status").s())) {
                out.add(item);
            }
        }
        return out;
    }

    // Azure Function: remove guest photos from Blob Storage.
    @FunctionName("removeGuestPhotos")
    public void removeGuestPhotos(List<String> guestIds, ExecutionContext context) {
        BlobServiceClient blobs = new BlobServiceClientBuilder()
            .connectionString(System.getenv("STORAGE"))
            .buildClient();
        BlobContainerClient photos = blobs.getBlobContainerClient("guest-photos");
        for (String guestId : guestIds) {
            photos.getBlobClient(guestId + ".jpg").deleteIfExists();
        }
    }
}
