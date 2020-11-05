package pe.com.alonso360rn.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class S3ToSQSHandler implements RequestHandler<S3Event, String> {
	public S3ToSQSHandler() {
	}

	final AmazonSQS amazonSQS = AmazonSQSClientBuilder.defaultClient();

	private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/152405606140/S3ToSQSQueue";

	@Override
	public String handleRequest(S3Event event, Context context) {
		String bucketName = event.getRecords().get(0).getS3().getBucket().getName();
		String objectKey = event.getRecords().get(0).getS3().getObject().getKey();

		String answer = bucketName.concat("-").concat(objectKey);

		SendMessageRequest sendMessageRequest = new SendMessageRequest().withQueueUrl(QUEUE_URL).withMessageBody(answer);
		
		amazonSQS.sendMessage(sendMessageRequest);

		context.getLogger().log(answer);

		return answer;
	}
}