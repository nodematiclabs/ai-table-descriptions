import functions_framework
import vertexai

from google.cloud import bigquery
from vertexai.preview.generative_models import GenerativeModel, Part

def generate(prompt):
    # Get a prompt response from Gemini (Vertex AI)
    model = GenerativeModel("gemini-pro")
    responses = model.generate_content(
        prompt,
        generation_config={
            "max_output_tokens": 512,
            "temperature": 0.9,
            "top_p": 1
        },
        stream=True,
    )

    responses = [response for response in responses]
    return responses[0].candidates[0].content.parts[0].text

# CloudEvent function to be triggered by an Eventarc Cloud Audit Logging trigger
# Note: this is NOT designed for second-party (Cloud Audit Logs -> Pub/Sub) triggers!
@functions_framework.cloud_event
def entrypoint(cloudevent):
    # Pull the table creation information from the payload
    payload = cloudevent.data.get("protoPayload")

    # Get the dataset_id and table_id from the resource_name
    dataset_id = payload.get('resourceName').split('/')[3]
    table_id = payload.get('resourceName').split('/')[5]

    # Initialize a BigQuery client
    client = bigquery.Client()

    # Construct a reference to the table
    table_ref = client.dataset(dataset_id).table(table_id)

    # Get table metadata
    table = client.get_table(table_ref)
    schema = table.schema
    column_list = ", ".join([field.name for field in schema])

    # Generate a table description
    table_description = generate(f"""For a table named "{table_id}", with columns "{column_list}", please write a short, one-sentence description for the table.""")

    # Generate column descriptions
    updated_schema = []
    for field in schema:
        column_description = generate(f"""For a table named "{table_id}", with columns "{column_list}" there is a column named "{field.name}" of type "{field.field_type}".  Please write a short, one-sentence description for this column.""")
        updated_schema.append(
            bigquery.SchemaField(
                field.name,
                field.field_type,
                description=column_description
            )
        )
    
    # Update the table
    table.description = table_description
    table.schema = updated_schema
    client.update_table(table, ["description", "schema"])