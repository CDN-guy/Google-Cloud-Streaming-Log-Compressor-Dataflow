### Google Cloud Streaming Log Compressor - Dataflow Flex Template
#### 1. Overview

This Dataflow Flex Template provides a scalable, streaming solution for processing and archiving logs from Google Cloud Logging.

The pipeline ingests log messages from a Pub/Sub subscription (fed by a Cloud Logging sink), batches them into customized, time-based windows, applies use-selected compression (gzip, bzip2, deflate) to each batch, and writes the compressed files to a Google Cloud Storage (GCS) bucket.

This is ideal for:

1. Cost-Effective Archival: Compressing logs significantly reduces GCS storage costs.

1. Batch Analysis: Grouping logs by time windows makes them easier to load into batch analytics systems.

1. Decoupling: Using Pub/Sub and Dataflow provides a resilient, at-least-once processing system that can handle backlogs and spikes in log volume.

#### 2. Prerequisites
Before deploying and running this template, you must have the following:

1. A Google Cloud Project: With billing enabled.

1. Google Cloud SDK (gcloud): Installed and authenticated (gcloud auth login).

1. Required APIs Enabled:

1. Dataflow API

1. Cloud Build API (Used to build the template)

1. Compute Engine API (Dataflow dependency)

1. Cloud Storage API

1. Pub/Sub API

1. Cloud Logging API

1. gcloud services enable dataflow.googleapis.com cloudbuild.googleapis.com compute.googleapis.com storage.googleapis.com pubsub.googleapis.com logging.googleapis.com

1. Required Permissions: Your user/principal needs roles like Dataflow Admin, Cloud Build Editor, Storage Admin, and Pub/Sub Admin to build and run the template.

1. Docker: Installed and running on your local machine to build the template image.

1. Apache Maven: (Or Gradle) To build the pipeline code.

#### 3. How to Deploy
Deploying a Flex Template is a two-step process:

1. Build & Stage the Template: You build a Docker container with your pipeline code and a Template Spec file, then store them in GCS.

2. Run the Template Job: You run a Dataflow job from the staged template, passing it your runtime parameters.

##### Step 1: Set up Cloud Resources
First, set up the necessary GCS buckets and Pub/Sub topics.

```
# --- 1. Set environment variables ---
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export TEMPLATE_BUCKET="your-template-bucket-name" # Must be globally unique
export LOG_OUTPUT_BUCKET="your-log-output-bucket" # Bucket for compressed logs
export LOGGING_SINK_TOPIC="your-log-sink-topic"
export PUBSUB_SUBSCRIPTION="your-pipeline-subscription"
export TEMPLATE_NAME="streaming-log-compressor"

gcloud config set project $PROJECT_ID
gcloud config set compute/region $REGION

# --- 2. Create GCS Buckets ---
# Bucket for staging the template
gsutil mb -p $PROJECT_ID -l $REGION gs://$TEMPLATE_BUCKET
# Bucket for pipeline output
gsutil mb -p $PROJECT_ID -l $REGION gs://$LOG_OUTPUT_BUCKET

# --- 3. Create Pub/Sub Topic and Subscription ---
gcloud pubsub topics create $LOGGING_SINK_TOPIC
gcloud pubsub subscriptions create $PUBSUB_SUBSCRIPTION \
  --topic=$LOGGING_SINK_TOPIC

# --- 4. Create a Logging Sink ---
# This example sink routes all global ERROR logs.
# **Customize the --log-filter** to match the logs you want to capture.
export SINK_NAME="dataflow-compressor-sink"
export SINK_DESTINATION="[pubsub.googleapis.com/projects/$PROJECT_ID/topics/$LOGGING_SINK_TOPIC](https://pubsub.googleapis.com/projects/$PROJECT_ID/topics/$LOGGING_SINK_TOPIC)"

gcloud logging sinks create $SINK_NAME $SINK_DESTINATION \
  --log-filter='severity="ERROR" resource.type="global"' \
  --description="Routes ERROR logs to Pub/Sub for Dataflow compressor"

# --- 5. Grant Pub/Sub publish permissions to the sink ---
# Find the sink's writerIdentity
export WRITER_IDENTITY=$(gcloud logging sinks describe $SINK_NAME --format='value(writerIdentity)')

# Grant the identity permission to publish to the topic
gcloud pubsub topics add-iam-policy-binding $LOGGING_SINK_TOPIC \
  --member=$WRITER_IDENTITY \
  --role='roles/pubsub.publisher'
```

##### Step 2: Build and Stage the Flex Template

This command packages your pipeline code into a Docker image, pushes it to Google Container Registry (GCR), and saves a template specification file in GCS.

```
# --- 1. Define template image location ---
export TEMPLATE_IMAGE_PATH="gcr.io/$PROJECT_ID/$TEMPLATE_NAME:latest"

# --- 2. Define the Template Spec path in GCS ---
export TEMPLATE_SPEC_PATH="gs://$TEMPLATE_BUCKET/templates/$TEMPLATE_NAME.json"

# --- 3. (Optional) Create a metadata.json file ---
# This file defines your pipeline's runtime parameters.
# Create a file named `metadata.json` in your project's root:
echo '{
  "name": "Streaming Log Compressor",
  "description": "Batches and GZIPs streaming logs from Pub/Sub to GCS.",
  "parameters": [
    {
      "name": "inputSubscription",
      "label": "Input Pub/Sub Subscription",
      "helpText": "The Pub/Sub subscription to read logs from. Format: projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>",
      "isOptional": false
    },
    {
      "name": "outputDirectory",
      "label": "Output GCS Directory",
      "helpText": "The GCS path to write compressed log files to. Must end with a trailing slash. Example: gs://my-bucket/logs/",
      "isOptional": false
    },
    {
      "name": "windowSize",
      "label": "Window Size (minutes)",
      "helpText": "The duration of the time window for batching logs, in minutes.",
      "isOptional": false,
      "defaultValue": 5
    }
  ]
}' > metadata.json


# --- 4. Build the template ---
# This command assumes a standard Java/Maven project structure.
# It reads your pom.xml to find the main class and dependencies.
gcloud dataflow flex-template build $TEMPLATE_SPEC_PATH \
  --image-gcr-path $TEMPLATE_IMAGE_PATH \
  --sdk-language "JAVA" \
  --flex-template-base-image "JAVA11" \
  --metadata-file "metadata.json" \
  --project $PROJECT_ID
```

Note: If your main class isn't specified in your pom.xml, you may need to add the --main-class "com.example.YourPipelineMainClass" flag.

#### Step 3: Run the Template Job
This command starts the streaming job on Dataflow using the template file you just built.
```
# --- 1. Set variables for the job ---
export JOB_NAME="streaming-log-compressor-$(date +%Y%m%d-%H%M%S)"
export INPUT_SUB_PATH="projects/$PROJECT_ID/subscriptions/$PUBSUB_SUBSCRIPTION"
export OUTPUT_GCS_PATH="gs://$LOG_OUTPUT_BUCKET/compressed-logs/"
export WINDOW_MINUTES=5

# --- 2. Run the job ---
gcloud dataflow flex-template run $JOB_NAME \
  --template-file-gcs-location $TEMPLATE_SPEC_PATH \
  --project $PROJECT_ID \
  --region $REGION \
  --parameters inputSubscription=$INPUT_SUB_PATH \
  --parameters outputDirectory=$OUTPUT_GCS_PATH \
  --parameters windowSize=$WINDOW_MINUTES
```
Your streaming pipeline is now running and will process logs as they arrive in the Pub/Sub subscription.

#### 4. Pipeline Parameters
| Parameter | Description | Required | Example |
| --- | --- | --- | --- |
| inputSubscription | The full path of the Pub/Sub subscription to read logs from | Yes | projects/my-project/subscriptions/my-sub | 
| outputDirectory | The GCS directory where compressed files will be written. Must end with a trailing slash (/). | Yes | gs://my-log-bucket/archives/ |
| windowSize | The duration of the time window for batching logs, in minutes. | Yes | 5 (for 5 minutes)|
| outputFilenamePrefix | (Optional) A prefix for the output files. | No | logs- |
