from dataclasses import dataclass
import pandas as pd
from .base import Base
from urllib.parse import quote 
from urllib.parse import urljoin
import requests

@dataclass
class AnswerOption(Base):
    option_value: str = None  # This can be adjusted to fit the data type (str, int, etc.)
    literal: str = None
    answer_option_uri: str = None
    questionnaire_item_uri: str = None  # To be used in the @reverse field

    def fhir_nexus_resource(self) -> dict:
        answer_option_resource = {}
        
#         # Questionnaire Item reference (this needs to be updated, done to create relation in Nexus)
#         if self.questionnaire_item_uri and pd.notna(self.questionnaire_item_uri):
#             answer_option_resource["questionnaire_item_name"] = {
#                 "reference": {"fhir:v": self.questionnaire_item_uri},
#                 "fhir:link": {"@id": self.questionnaire_item_uri}
#             }

        # Set the value[x] - using valueString for this example
        if self.option_value and pd.notna(self.option_value):
            if isinstance(self.option_value, int):
                answer_option_resource["valueInteger"] = {"fhir:v": self.option_value}
            else:
                answer_option_resource["valueString"] = {"fhir:v": str(self.option_value)}

        # Store the 'literal' as an extension field within AnswerOption
        if self.literal and pd.notna(self.literal):
            literal_extension = {
                "url": {"fhir:v":"http://yourorganization.com/fhir/StructureDefinition/displayLiteral"},
                "valueString": {"fhir:v": self.literal}
            }
            # Check if 'extension' key already exists in answer_option_resource
            if "extension" in answer_option_resource:
                answer_option_resource["extension"].append(literal_extension)
            else:
                answer_option_resource["extension"] = [literal_extension]

        # Formulate nexus_dict
        nexus_dict = {
            "@context": [
                {"@vocab": "http://hl7.org/fhir/"},
                "https://bluebrain.github.io/nexus/contexts/metadata.json",
                {"fhir": "http://hl7.org/fhir/"}
            ],
            "@id": self.answer_option_uri,
            "@type": "fhir:Questionnaire.Item.AnswerOption",
            "@reverse": {
                "fhir:Questionnaire.Item.AnswerOption": {
                     "@id": self.questionnaire_item_uri                                     
                    }
            }
        }

        nexus_dict.update(answer_option_resource)

        return nexus_dict

    def update_to_nexus(self, nexus_url, org, project,token):
        answer_option_uri= f"{self.answer_option_uri}"
        # URL encode the answer_option_uri
        answer_option_uri_encoded = quote(answer_option_uri, safe='')
        
        resource_url = f"{nexus_url}/resources/{org}/{project}/_/{answer_option_uri_encoded}"
        
        # Define headers with the authentication token
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # Try fetching the resource
        try:
            fetch_response = requests.get(resource_url, headers=headers)
            fetch_response.raise_for_status()
            old_nexus_dict = fetch_response.json()

            # Prepare data for updating
            nexus_data = self.fhir_nexus_resource()
            
            previous_rev = old_nexus_dict['_rev']
            # Construct the URL with the revision number
            update_resource_url = f"{resource_url}?rev={previous_rev}"

            # Attempt to update the resource
            try:
                update_response = requests.put(update_resource_url, json=nexus_data, headers=headers)
                update_response.raise_for_status()
                print("Updated")
            except requests.exceptions.HTTPError as e:
                print(f"Failed to update: {e}")

        except requests.exceptions.HTTPError as e:
            # If fetching fails, try creating the resource
            print("Inserting new resource")
            try:
                resource_url = f"{nexus_url}/resources/{org}/{project}/_"
                create_response = requests.post(resource_url, json=self.fhir_nexus_resource(),  headers=headers)
                create_response.raise_for_status()
                print("Resource successfully created.")
            except requests.exceptions.HTTPError as e:
                print(f"Creation Failed: {e}")
