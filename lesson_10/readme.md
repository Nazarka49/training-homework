gcloud init:
Description: Initializes, authorizes, and configures the gcloud tool. This command helps you set up the CLI with your Google account and select a default project.
Usage: gcloud init



gcloud auth login:
Description: Authenticates a Google Cloud account via a web-based authorization flow. This command is essential for setting up your CLI to act on behalf of your Google Cloud account.
Usage: gcloud auth login

gsutil config

gcloud config set project PROJECT_ID

gcloud projects list:
Description: Lists all Google Cloud projects accessible to your currently authenticated account. This is useful for viewing what projects you have access to.
Usage: gcloud projects list

gcloud config set project [PROJECT_ID]:
Description: Sets the default project for all subsequent commands in your current session. This is crucial when working across multiple projects.
Usage: gcloud config set project your-project-id

gsutil mb:
Description: Creates a new bucket in Google Cloud Storage. You must specify a globally unique name and optionally a storage class and location.
Usage: gsutil mb gs://my-data-backet

gsutil ls:
Description: Lists all buckets under your current project or lists the contents of a specific bucket.
Usage:
List all buckets: gsutil ls
List contents of a specific bucket: gsutil ls gs://my-data-backet

gsutil cp:
Description: Copies files to and from Google Cloud Storage buckets. This command can be used for uploading, downloading, or copying files between buckets.
Usage:
Upload a file: gsutil cp local-file.txt gs://your-bucket-name
Download a file: gsutil cp gs://my-data-backet/remote-file.txt ./local-directory

gsutil rm:
Description: Deletes an object from a bucket. If you need to delete multiple objects, you can use wildcards or a recursive flag.
Usage:
Delete a single file: gsutil rm gs://your-bucket-name/file.txt
Delete all files in a bucket: gsutil rm gs://my-data-backet/**

gsutil rb:
Description: Removes an empty bucket. The bucket must be empty before it can be deleted.
Usage: gsutil rb gs://your-bucket-name

gsutil iam ch:
Description: Changes the IAM policy of a bucket, such as adding or removing user permissions.
Usage: gsutil iam ch user:email@example.com:roles/storage.objectViewer gs://your-bucket-name

gsutil versioning set on:
Description: Enables versioning for a bucket, allowing you to keep a history of objects as they are overwritten or deleted.
Usage: gsutil versioning set on gs://your-bucket-name

gsutil lifecycle set lifecycle-config.json gs://your-bucket-name:
Description: Sets the lifecycle management policy for a bucket, which can automate tasks like deleting old objects or transitioning them to cheaper storage classes.
Usage:
First, you need a JSON file defining the lifecycle rules: lifecycle-config.json
Apply the lifecycle rules: gsutil lifecycle set lifecycle-config.json gs://your-bucket-name


AWS

General
aws configure: Sets up AWS CLI with your credentials and default region.
aws help: Provides help content and descriptions of the CLI commands and their options.

aws configure set region us-east-1

List Buckets
Command: aws s3 ls
Description: Lists all your S3 buckets.
Example: aws s3 ls

List Contents of a Bucket
Command: aws s3 ls s3://bucket-name
Description: Lists the contents of a specified bucket.
Example: aws s3 ls s3://my-bucket

Create a Bucket
Command: aws s3 mb s3://bucket-name
Description: Creates a new bucket. Bucket names must be unique across all existing bucket names in Amazon S3.
Example: aws s3 mb s3://my-new-bucket

Delete a Bucket
Command: aws s3 rb s3://bucket-name
Description: Deletes an empty S3 bucket. To delete a bucket that contains objects, you must first delete all objects and subfolders.
Example: aws s3 rb s3://my-old-bucket

File and Object Operations

Upload a File
Command: aws s3 cp local-file.txt s3://bucket-name/
Description: Uploads a local file to an S3 bucket. You can specify a different object name within the bucket if desired.
Example: aws s3 cp ./example.txt s3://my-bucket/

Download a File
Command: aws s3 cp s3://bucket-name/file.txt local-file.txt
Description: Downloads a file from S3 to your local machine.
Example: aws s3 cp s3://my-bucket/example.txt ./example-download.txt

Delete a File
Command: aws s3 rm s3://bucket-name/file.txt
Description: Deletes a specific object from a bucket.
Example: aws s3 rm s3://my-bucket/example.txt

Copy a File within S3
Command: aws s3 cp s3://bucket-name/source.txt s3://bucket-name/destination.txt
Description: Copies a file within the same bucket or across different buckets.
Example: aws s3 cp s3://my-bucket/example.txt s3://my-bucket/archive/example.txt

Move a File within S3
Command: aws s3 mv s3://bucket-name/source.txt s3://bucket-name/destination.txt
Description: Moves a file within the same bucket or across different buckets. This is effectively a copy followed by a delete of the original object.
Example: aws s3 mv s3://my-bucket/example.txt s3://my-bucket/archive/example.txt

Advanced S3 Operations

Enable Versioning on a Bucket
Command: aws s3api put-bucket-versioning --bucket bucket-name --versioning-configuration Status=Enabled
Description: Enables versioning for the specified bucket to keep multiple versions of an object in one bucket.
Example: aws s3api put-bucket-versioning --bucket my-bucket --versioning-configuration Status=Enabled


Azure:

Here's a list of commonly used Azure CLI commands along with brief descriptions:

General Commands
az login: Logs in to Azure interactively or through device authentication.
az logout: Logs out from the current Azure session.
az account list: Lists all Azure subscriptions associated with the logged-in account.
az account set --subscription <subscription_id>: Sets the active subscription for subsequent commands.
