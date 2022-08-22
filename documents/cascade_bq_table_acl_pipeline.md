## Automatically cascade data access policy to derived tables based on data lineage using BigQuery, DataCatalog, and Dataflow

This tutorial demonstrates how to implement a data lineage use case in which
you apply the same column-level access control lists (ACLs) and metadata tags to
derived tables in [BigQuery](https://cloud.google.com/bigquery).

Refer to the general concept for building [Data lineage systems for a data warehouse](https://cloud.google.com/solutions/architecture-concept-data-lineage-systems-in-a-data-warehouse) and
[Building a BigQuery data lineage solution Using audit logs, PubSub, ZetaSQL, Dataflow and Data Catalog](https://cloud.google.com/solutions/building-a-bigquery-data-lineage-solution).

Note: To complete this tutorial, you must have completed the [BigQuery lineage extraction tutorial](bigquery_lineage_pipeline.md).

In this tutorial, you use data lineage to enforce [column-level access controls](https://cloud.google.com/bigquery/docs/column-level-security-intro#column-level_security_workflow) policies
in real time by deploying a Dataflow streaming pipeline to
cascade policy tags. The pipeline monitors for new BigQuery
tables and applies the sensitive policy and metadata tags in the source tables
to the derived tables by using [BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables)
and [Data Catalog API](https://cloud.google.com/data-catalog/docs/api-libraries-overview). The derived table is associated with source tables
through [data lineage](https://cloud.google.com/solutions/architecture-concept-data-lineage-systems-in-a-data-warehouse.md) information that is extracted through a [separate pipeline](bigquery_lineage_pipeline.md).

This tutorial is intended for people who are responsible for metadata management,
data governance, and related analytics. This tutorial assumes that you have basic
knowledge of building Dataflow pipelines using basic shell,
[Apache Beam Java SDK](https://beam.apache.org/get-started/beam-overview/), [Data Catalog](https://cloud.google.com/data-catalog),
and [BigQuery](https://cloud.google.com/bigquery).

### Architecture

The following diagram shows the Dataflow streaming pipeline that
you deploy in this tutorial.

![Data lineage processing pipeline.](https://cloud.google.com/solutions/images/lineage-based-access-cascading.svg)

The following list explains the flow of events in the architecture:

1.  The Dataflow streaming pipeline uses the BigQuery API
    to identify [column-level access controls](https://cloud.google.com/bigquery/docs/column-level-security-intro#column-level_security_workflow) applied to the source table columns of a BigQuery
    table based on the table data lineage information.
1.  The Dataflow streaming pipeline applies the source
    policies to the output table columns. The policies are mapped to the column
    based on column-level lineage.

### Concepts

A [Data Catalog taxonomy](https://cloud.google.com/data-catalog/docs/reference/rpc/google.cloud.datacatalog.v1beta1#taxonomy) 
is a collection of [policy tags](https://cloud.google.com/bigquery/docs/best-practices-policy-tags) 
that classifies data against a common axis- for example, a data sensitivity taxonomy
could contain policy tags denoting personally identifiable information (PII) such
as a postal code or tax identifier. A data origin taxonomy could contain
policy tags to classify assets such as user data, employee data, partner data, and
public data. You can use taxonomy and policy tags to enforce 
[column-level access control](https://cloud.google.com/bigquery/docs/column-level-security-intro) 
for BigQuery tables by configuring the identity and access management (IAM) permissions for the tags.

Policy tags taxonomies and Data Catalog tags are both created in
Data Catalog, but you must apply different APIs. You use
BigQuery API to apply policy tags to tables, using the 
[`setCategory`](https://cloud.google.com/bigquery/docs/access-control#bq-permissions) permission. 
You use the [Data Catalog API](https://cloud.google.com/data-catalog/docs/api-libraries-overview) to apply Data Catalog tags.

### Objectives

-   Create taxonomies in Data Catalog.
-   Learn about BigQuery column-level ACLs using policy tags.
-   Understand how to use extracted data lineage in a downstream system.

## Setting the environment

The scripts and commands used in this tutorial rely on shell environment
variables.

1.  In Shell, set the environment variables for your project ID:

        export PROJECT_ID=$(gcloud config get-value project)
        export LINEAGE_PROJECT_ID="EXTRACTION_PROJECT_ID"

    Replace `extraction-project-id` with the Google Cloud project ID of the data lineage extraction pipeline that you
    created in [Building a BigQuery data lineage solution Using audit logs, PubSub, ZetaSQL, Dataflow and Data Catalog](https://cloud.google.com/solutions/building-a-bigquery-data-lineage-solution).

1.  Set the environment variable for the Google Cloud region:

        export REGION_ID="cloud-region"

    For data localization purposes, replace the `cloud-region`
    placeholder variable with a [Dataflow regional endpoint](https://cloud.google.com/dataflow/docs/concepts/regional-endpoints).

## Configuring a data taxonomy

Data Catalog provides policy tags to create a set of categories
for classifying sensitive data. The policy tags also enforce category-specific
access control. In this section, you build a taxonomy for sensitive data and create
a PII category with `Telephone number` as a subcategory.

### Create policy tags

1.  In the Cloud Console,go to the [**Create and manage policy tags**](https://console.cloud.google.com/datacatalog/taxonomies) on the [**Data Catalog**](https://console.cloud.google.com/datacatalog) page.

1.  Click **+Create**.

    As shown in the following example image, you must populate the fields with
    the listed information:

    **Taxonomy name**: `Privacy`
    **Description**: `Sensitive end-user information`
    **Project**: `lineage-testing-301806`

    ![New policy tag taxonomy fields.](https://cloud.google.com/solutions/images/new_policy_tag.png)

1.  In the **Taxonomy Name** field, enter `Privacy`.
1.  In the **Policy tag** field, enter`pii_telephone`.
1.  Click **Save**.

### Enumerate the monitored policy tag ID

The tag cascading pipeline that you create in this tutorial cascades policy
tags from parent tables to generated or derived tables. The cascading pipeline
checks the source table policy tags against the list of policy tags that you
provide. The pipeline applies matching policy tags to destination tables using
the column-level mapping of data lineage.

In this section, you configure the pipeline to monitor for the tag you created in
the previous section.

1.  In the Google Cloud console, go to the **Taxonomies**
    page.

    [Go to Taxonomies](https://console.cloud.google.com/datacatalog/taxonomies)

    Copy the ID for the `pii_telephone` policy tag. The format of the policy tag
    ID is as follows:

        projects/<project-id>/locations/<region>/taxonomies/<NUMERIC_TAXONOMY_ID>/policyTags/<NUMERIC_POLICY_ID>

1.  In Cloud Shell, set the policy tag ID as an environment variable:

        export MONITORED_POLICY_TAG_ID="COPIED_POLICY_TAG_ID"

## Setting up a test environment

In this step, you load sample PII data into a BigQuery table and
apply a policy tag to one of the PII columns.

### Load sample PII data

In this section, you load mock PII data into a table and name it `MockPiiData`.
This data has multiple columns that contain example sensitive information such as
telephone numbers and email addresses.

1.  In Cloud Shell, create, create a new dataset:

        bq --project_id=$PROJECT_ID mk --dataset MyDataSet

1.  Create a new table with sample PII data:

        bq --project_id=$PROJECT_ID load \
        --autodetect \
        --source_format=CSV \
        MyDataSet.MockPiiTable \
        mock_pii_data.csv

### Apply a policy tag

In this section, you apply safeguards to the sample PII data which resemble the safeguards
that you apply to real data. For this tutorial, you can [apply the test policy tag](https://cloud.google.com/bigquery/docs/column-level-security#bq) through the BigQuery command-line tool or on
the BigQuery web console.

1.  In Cloud Shell, download the BigQuery table
    schema:

        bq show --schema --format=prettyjson \
        $PROJECT_ID:MyDataSet.MockPiiTable > mock_pii_table_schema.json

1.  Update the policy tag in the BigQuery table schema:

        ./add_policy_tag.py mock_pii_table_schema.json phone_number \
        $MONITORED_POLICY_TAG_ID

1.  Update the schema in BigQuery:

        bq update \
        $PROJECT_ID:MyDataSet.MockPiiTable mock_pii_table_schema.json

## Enabling audit logging

Operation logs in Google Cloud are captured centrally using
[Cloud Logging](https://cloud.google.com/logging).
In this section, you set up the export of your Cloud project `data_access`
logs to the lineage extraction pipeline for processing.

### Export logs to PubSub

1.  In Cloud Shell, replace the `TOPIC_ID`
    placeholder variable with the name of the PubSub topic that you
    created in [part two of this series](https://cloud.google.com/solutions/building-a-bigquery-data-lineage-solution):

        export AUDIT_LOGS_PUBSUB_TOPIC="TOPIC_ID"

    This command enables the tag cascading pipeline to access data lineage
    information.

1.   Create the log sink:

         export LOG_SINK_ID=NAME_OF_LOG_SINK
         gcloud logging sinks create $LOG_SINK_ID \
             pubsub.googleapis.com/projects/$LINEAGE_PROJECT_ID/topics/$AUDIT_LOGS_PUBSUB_TOPIC \
             --log-filter='protoPayload.metadata."@type"="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata"
         protoPayload.methodName="google.cloud.bigquery.v2.JobService.InsertJob" operation.last=true'

     Replace the `name-of-log-sink` placeholder variable
     with a unique name for the sink of your choice.

1.  Give the `pubsub.publisher` role to the Cloud Logging service account:

        # Identify the Logs writer service account
        export LOGGING_WRITER_IDENTITY=$(gcloud logging sinks describe $LOG_SINK_ID --format="get(writerIdentity)" --project $PROJECT_ID)

        # Grant Publish permission to the Logging writer
        gcloud pubsub topics add-iam-policy-binding $AUDIT_LOGS_PUBSUB_TOPIC \
            --member=$LOGGING_WRITER_IDENTITY \
            --role='roles/pubsub.publisher' \
            --project $LINEAGE_PROJECT_ID

    This command grants the Cloud Logging service account permission to
    publish log entries.

1.  Verify that the permission is granted:

        gcloud pubsub topics get-iam-policy $AUDIT_LOGS_PUBSUB_TOPIC --project $LINEAGE_PROJECT_ID

    The output lists permissions, including the `LOGGING_WRITER_IDENTITY` permission.

## Running the tag cascading pipeline

The tag cascading pipeline then reads the policy tags for each of the parent columns
and applies them to the new table columns for the tags in the monitored list.

The Dataflow runner needs BigQuery
`bigquery.tables.setCategory` permission to apply policy tags to a table. This
permission is also part of the `roles/bigquery.dataOwner` BigQuery
predefined role.

### Grant permissions to Dataflow runners

The tag cascading pipeline uses the Compute Engine service account to access
Google Cloud services. In this section, you give the Dataflow
runners permission to read lineage from the PubSub topic hosted
in the extraction project and manipulate BigQuery policy tags.

1.  In Cloud Shell, identify Compute Engine default service
    account for the tag cascading project:

        export CE_SERVICE_ACCOUNT=$(gcloud iam service-accounts list | grep -Eo "[0-9]+-compute@[a-z.]+")

1.  Grant the Compute Engine service account permission to
    subscribe to the PubSub topic in a different project:

        gcloud projects add-iam-policy-binding $LINEAGE_PROJECT_ID \
            --member="serviceAccount:$CE_SERVICE_ACCOUNT" \
            --role="roles/pubsub.editor"

1.  Identify the Compute Engine account for the lineage extraction
    pipeline:

        export EXTRACTION_CE_SERVICE_ACCOUNT=$(gcloud iam service-accounts list --project $LINEAGE_PROJECT_ID | grep -Eo "[0-9]+-compute@[a-z.]+")

1.  Grant the lineage extraction pipeline permission to read the data from
    the BigQuery table in your project:

        gcloud projects add-iam-policy-binding $PROJECT_ID \
            --member="serviceAccount:$EXTRACTION_CE_SERVICE_ACCOUNT" \
            --role="roles/bigquery.dataViewer"

    The lineage extraction pipeline needs permission to read schema information
    for lineage.

### BigQuery policy tags 

To implement the permissions for the runner, in this section you create a
custom Cloud IAM role with the `bigquery.tables.setCategory` permission.

1.  In Cloud Shell, create a custom IAM role:

        gcloud iam roles create bigquery_policy_tags_admin \
            --project=$PROJECT_ID \
            --file=bigquery-policy-tags-admin.yaml

    This command uses the file `bigquery-policy-tags-admin.yaml` to describe
    a custom role having `bigquery.tables.setCategory` permission. After this
    permission is granted to a role, the role can manipulate policy tags. The
    custom role permissions are as follows:

        # [START policy_permissions_info]
        title: "BigQuery Policy Tags Admin"
        description: "Allow users to read and update Column Policy Tags"
        stage: "BETA"
        includedPermissions:
        - bigquery.tables.setCategory
        # [END policy_permissions_info]

    The output is similar to the following:

    <pre>
        Created role [bigquery_policy_tags_admin].
        description: Permit using BigQuery policy tags for lineage-based propagation.
        etag: BwWm_eD6GI0=
        includedPermissions:
        - bigquery.tables.setCategory
        name: projects/bigquery-lineage-demo/roles/bigquery_policy_tags_admin
        stage: GA
        title: Policy Tags Admin
    </pre>

1.  Apply the custom Cloud IAM role to the Compute Engine service account:

        gcloud projects add-iam-policy-binding $PROJECT_ID \
            --member="serviceAccount:$CE_SERVICE_ACCOUNT" \
            --role="projects/$PROJECT_ID/roles/bigquery_policy_tags_admin"

    The custom role enables the service account to edit the BigQuery
    policy tags.

## Start the tag cascading pipeline

You can now run the policy propagation.

1.  In Cloud Shell, create a Cloud Storage bucket for the pipeline staging
    location by replacing `TEMP_GCS_BUCKET_NAME` with a
    new name of your choice for the Cloud Storage bucket:

        export TEMP_GCS_BUCKET="TEMP_GCS_BUCKET_NAME"
        gsutil mb -p $PROJECT_ID -l $REGION_ID gs://$TEMP_GCS_BUCKET

1.  Identify the exported lineage PubSub topic and replace `LINEAGE_EXPORT_TOPIC` with the data lineage export topic ID:

        export LINEAGE_OUTPUT_PUBSUB_TOPIC="LINEAGE_EXPORT_TOPIC"

1.  Build the pipeline using Cloud Build

    ```shell
    gcloud builds submit \
    --substitutions _JAR_GCS_LOCATION="${TEMP_GCS_BUCKET}/jars" \
    --project "${PROJECT_ID}"
    ```

1.  Download the JAR file:

    ```shell
    gsutil cp "gs://${TEMP_GCS_BUCKET}/jars/bigquery-data-lineage-bundled-0.1-SNAPSHOT.jar" .
    ```
    
1.  Launch the Dataflow pipeline:

    ```shell
    CASCADE_MAIN_CLASS="com.google.cloud.solutions.datalineage.PolicyPropagationPipeline"
    
    java -cp bigquery-data-lineage-bundled-0.1-SNAPSHOT.jar \
     "${CASCADE_MAIN_CLASS}" \
    --streaming=true \
    --project"=${PROJECT_ID}" \
    --runner=DataflowRunner \
    --gcpTempLocation="gs://${TEMP_GCS_BUCKET}/temp/" \
    --stagingLocation="gs://${TEMP_GCS_BUCKET}/staging/" \
    --workerMachineType=n1-standard-1 \
    --region="${REGION_ID}" \
    --lineagePubSubTopic="projects/${LINEAGE_PROJECT_ID}/topics/${LINEAGE_OUTPUT_PUBSUB_TOPIC}" \
    --monitoredPolicyTags="${MONITORED_POLICY_TAG_ID}"
    ```

    The command initializes a Dataflow streaming pipeline
    that reads the lineage data exported to the provided PubSub
    topic. The streaming pipeline searches the source columns for
    `monitoredPolicyTags` tags and applies the tags to derived columns in the
    target table.

    Repeat the`--monitoredPolicyTags` parameter once for each of the monitored
    policy tags.

    The following diagram shows the processing steps you see in Cloud Console
    Dataflow.

    ![Tag cascading pipeline process flow.](https://cloud.google.com/solutions/images/tag-cascading-pipeline.png)

## Testing the tag cascading pipeline

To verify that the tag cascading pipeline is propagating the monitored tags
correctly, create a new table by querying the `MockPii` table.

1.  In the Cloud Console, go to the BigQuery page.

    <a href="https://console.cloud.google.com/bigquery"+
    target="console" class="button button-primary" track-type="quickstart"
    track-name="consoleLink">
    Go to BigQuery</a>

1.  To set the destination table for query results, click **More**, then
    **Query Settings**.

1.  Choose **Set a destination table**, and enter the table name as`MyOutputTable`.

1.  Enter the following code into the query editor and click **Run query**:

        #standardSQL
        SELECT
          CONCAT(first_name,' ', last_name) AS full_name,
          phone_number AS imsi
        FROM
          `<project-id>.MyDataSet.MockPiiTable`

1.  After the query execution successfully completes, the **imsi** column
    of the **MyOutputTable** table has the `pii_telephone` policy tag.

    It can take a few minutes for the policy tags to appear in BigQuery
    because of inherent latencies in the query execution audit log and the
    Dataflow processing pipeline.


