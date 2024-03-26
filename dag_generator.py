import os
import json

# Load the template file
with open("template.py", "r") as template_file:
    template_content = template_file.read()

# Define the folder path containing JSON files
json_folder = "./jobs/"

# Create the "dags" folder if it doesn't exist
if not os.path.exists("dags"):
    os.makedirs("dags")

# Loop through each JSON file in the folder
for json_file in os.listdir(json_folder):
    if json_file.endswith(".json"):
        json_path = os.path.join(json_folder, json_file)
        dag_name = os.path.splitext(json_file)[0]

        # Read JSON content from file
        with open(json_path, "r") as file:
            json_content = json.load(file)

        # Inject JSON content into the template
        dag_content = template_content.replace("{{jar}}", json.dumps(json_content["jar"]))
        dag_content = dag_content.replace("{{spark_submit_parameters}}", json.dumps(json_content["spark_submit_parameters"]))
        dag_content = dag_content.replace("{{arguments}}", json.dumps(json_content["arguments"]))

        # Convert the value of the "job" key to a JSON string before replacing it in the template
        dag_content = dag_content.replace("{{ job }}", json.dumps(json_content["job"]))

        # Create a new Python file with DAG content in the "dags" folder
        new_python_file = f"../dags/{dag_name}_dag.py"
        with open(new_python_file, "w") as new_file:
            new_file.write(dag_content)

        print(f"DAG file created: {new_python_file}")
