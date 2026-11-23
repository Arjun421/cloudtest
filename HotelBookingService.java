// Hotel booking service: AWS Lambda + Azure Functions (Java test file for CloudCostGuard).
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import com.amazonaws.services.lambda.runtime.Context;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;
import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;

public class HotelBookingService {
    private static final String BUCKET = "hotel-invoices";
    private static final String TABLE = "bookings";
    private static final String QUEUE_URL = System.getenv("CONFIRM_QUEUE_URL");

    private static final int DDB_BATCH_GET_LIMIT = 100;
    private static final int SQS_BATCH_LIMIT = 10;
    private static final int BLOB_BATCH_LIMIT = 256;
    private static final int MAX_RETRY_ATTEMPTS = 8;

    // Clients are created once and reused across warm invocations.
    private static final S3Client S3 = S3Client.create();
    private static final SqsClient SQS = SqsClient.create();
    private static final DynamoDbClient DDB = DynamoDbClient.create();

    // Shared pool for concurrent S3 uploads (S3 has no multi-object PUT API).
    private static final ExecutorService S3_UPLOADS = Executors.newFixedThreadPool(8, r -> {
        Thread t = new Thread(r, "s3-upload");
        t.setDaemon(true);
        return t;
    });

    // Azure Blob clients are created lazily once and reused across invocations.
    private static final class BlobHolder {
        static final BlobServiceClient BLOBS = new BlobServiceClientBuilder()
            .connectionString(System.getenv("STORAGE"))
            .buildClient();
        static final BlobContainerClient PHOTOS = BLOBS.getBlobContainerClient("guest-photos");
        static final BlobBatchClient BATCH = new BlobBatchClientBuilder(BLOBS).buildClient();
    }

    // AWS Lambda entry point: save invoices, confirm bookings, count archived invoices.
    public int handleRequest(Map<String, List<String>> event, Context context) {
        List<String> bookingIds = event.get("bookingIds");

        // Load every booking using BatchGetItem (up to 100 keys per request).
        List<String> uniqueIds = new ArrayList<>(new LinkedHashSet<>(bookingIds));
        Map<String, Map<String, AttributeValue>> byId = new HashMap<>();
        for (int i = 0; i < uniqueIds.size(); i += DDB_BATCH_GET_LIMIT) {
            List<Map<String, AttributeValue>> keys = new ArrayList<>();
            for (String id : uniqueIds.subList(i, Math.min(i + DDB_BATCH_GET_LIMIT, uniqueIds.size()))) {
                keys.add(Map.of("id", AttributeValue.fromS(id)));
            }
            Map<String, KeysAndAttributes> request =
                Map.of(TABLE, KeysAndAttributes.builder().keys(keys).build());
            int attempt = 0;
            while (request != null && !request.isEmpty()) {
                BatchGetItemResponse resp = DDB.batchGetItem(
                    BatchGetItemRequest.builder().requestItems(request).build());
                for (Map<String, AttributeValue> item : resp.responses().getOrDefault(TABLE, List.of())) {
                    byId.put(item.get("id").s(), item);
                }
                request = resp.unprocessedKeys();
                if (request != null && !request.isEmpty()) {
                    if (++attempt > MAX_RETRY_ATTEMPTS) {
                        throw DynamoDbException.builder()
                            .message("Unprocessed keys remained after " + MAX_RETRY_ATTEMPTS + " retries")
                            .build();
                    }
                    backoff(attempt);
                }
            }
        }
        List<Map<String, AttributeValue>> bookings = new ArrayList<>();
        for (String id : bookingIds) {
            bookings.add(byId.getOrDefault(id, Map.of()));
        }

        // Upload one invoice per booking. S3 has no batch PUT API, so uploads run
        // concurrently on a shared pool with the single reused client.
        List<CompletableFuture<Void>> uploads = bookingIds.stream()
            .map(id -> CompletableFuture.runAsync(() -> uploadInvoice(id), S3_UPLOADS))
            .collect(Collectors.toList());
        try {
            CompletableFuture.allOf(uploads.toArray(new CompletableFuture[0])).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw e;
        }

        // Send confirmation messages in batches of up to 10.
        for (int i = 0; i < bookingIds.size(); i += SQS_BATCH_LIMIT) {
            List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
            int end = Math.min(i + SQS_BATCH_LIMIT, bookingIds.size());
            for (int j = i; j < end; j++) {
                entries.add(SendMessageBatchRequestEntry.builder()
                    .id(Integer.toString(j))
                    .messageBody(bookingIds.get(j))
                    .build());
            }
            int attempt = 0;
            while (!entries.isEmpty()) {
                SendMessageBatchResponse resp = SQS.sendMessageBatch(SendMessageBatchRequest.builder()
                    .queueUrl(QUEUE_URL)
                    .entries(entries)
                    .build());
                if (resp.failed().isEmpty()) {
                    break;
                }
                Set<String> failedIds = new HashSet<>();
                for (BatchResultErrorEntry failed : resp.failed()) {
                    if (Boolean.TRUE.equals(failed.senderFault())) {
                        throw SqsException.builder()
                            .message("Failed to send message " + failed.id() + ": " + failed.code() + " " + failed.message())
                            .build();
                    }
                    failedIds.add(failed.id());
                }
                if (++attempt > MAX_RETRY_ATTEMPTS) {
                    throw SqsException.builder()
                        .message("Failed to send " + failedIds.size() + " messages after " + MAX_RETRY_ATTEMPTS + " retries")
                        .build();
                }
                List<SendMessageBatchRequestEntry> retry = new ArrayList<>();
                for (SendMessageBatchRequestEntry entry : entries) {
                    if (failedIds.contains(entry.id())) {
                        retry.add(entry);
                    }
                }
                entries = retry;
                backoff(attempt);
            }
        }

        // Count archived invoices.
        int count = 0;
        for (ListObjectsV2Response listing : S3.listObjectsV2Paginator(b -> b.bucket(BUCKET).prefix("invoices/"))) {
            count += listing.keyCount();
        }
        return count;
    }

    // Uploads a single invoice using the shared S3 client.
    private static void uploadInvoice(String id) {
        S3.putObject(b -> b.bucket(BUCKET).key("invoices/" + id + ".pdf"), RequestBody.fromString("invoice " + id));
    }

    // Find all cancelled bookings.
    public List<Map<String, AttributeValue>> cancelledBookings(DynamoDbClient ddb) {
        List<Map<String, AttributeValue>> out = new ArrayList<>();
        for (ScanResponse resp : ddb.scanPaginator(b -> b.tableName(TABLE))) {
            for (Map<String, AttributeValue> item : resp.items()) {
                if ("cancelled".equals(item.get("status").s())) {
                    out.add(item);
                }
            }
        }
        return out;
    }

    // Azure Function: remove guest photos from Blob Storage.
    @FunctionName("removeGuestPhotos")
    public void removeGuestPhotos(List<String> guestIds, ExecutionContext context) {
        BlobContainerClient photos = BlobHolder.PHOTOS;
        String containerName = photos.getBlobContainerName();
        for (int i = 0; i < guestIds.size(); i += BLOB_BATCH_LIMIT) {
            List<String> chunk = new ArrayList<>();
            for (String guestId : guestIds.subList(i, Math.min(i + BLOB_BATCH_LIMIT, guestIds.size()))) {
                chunk.add(guestId + ".jpg");
            }
            int attempt = 0;
            while (!chunk.isEmpty()) {
                BlobBatch batch = BlobHolder.BATCH.getBlobBatch();
                List<Response<Void>> responses = new ArrayList<>();
                for (String blobName : chunk) {
                    responses.add(batch.deleteBlob(containerName, blobName));
                }
                BlobHolder.BATCH.submitBatchWithResponse(batch, false, null, com.azure.core.util.Context.NONE);
                List<String> failed = new ArrayList<>();
                RuntimeException firstError = null;
                for (int k = 0; k < chunk.size(); k++) {
                    int status;
                    try {
                        status = responses.get(k).getStatusCode();
                    } catch (RuntimeException e) {
                        if (firstError == null) {
                            firstError = e;
                        }
                        status = -1;
                    }
                    // 404 means the blob did not exist (same as deleteIfExists returning false).
                    if (status != 404 && (status < 200 || status >= 300)) {
                        failed.add(chunk.get(k));
                    }
                }
                if (failed.isEmpty()) {
                    break;
                }
                if (++attempt > MAX_RETRY_ATTEMPTS) {
                    if (firstError != null) {
                        throw firstError;
                    }
                    throw new IllegalStateException("Failed to delete " + failed.size()
                        + " blobs after " + MAX_RETRY_ATTEMPTS + " retries: " + failed);
                }
                // Retry only the failed deletions in a new batch request.
                chunk = failed;
                backoff(attempt);
            }
        }
    }

    private static void backoff(int attempt) {
        try {
            Thread.sleep(Math.min(1000L, 50L << Math.min(attempt, 10)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying batch request", e);
        }
    }
}
