from dataclasses import dataclass
import datetime
import pandas as pd
from .base import Base
from datetime import date
from urllib.parse import quote 
from urllib.parse import urljoin
import requests

@dataclass
class QuestionnaireResponseItem(Base):
    questionnaire_item_uri: str = None
    question: str = None
    value: str = None
    data_type: str = None
    response_item_uri: str = None
    ques_response_uri: str = None
 
        
#     @staticmethod
#     def from_row(row):
#         """
#         Factory method to create an instance from a DataFrame row.
#         """
#         return QuestionnaireResponseItem(
#             questionnaire_item_uri=row.get('questionnaire_item_uri'),
#             question=row.get('question'),
#             value=row.get('value'),
#             data_type=row.get('data_type'),
#             response_item_uri=row.get('response_item_uri'),
#             ques_response_uri=row.get('ques_response_uri')
#         )

    def fhir_nexus_resource(self) -> dict:
        response_item_resource = {}
        
        
#         # Questionnaire Response reference (this needs to be updated, done to create relation in Nexus)
#         if self.ques_response_uri and pd.notna(self.ques_response_uri):
#             response_item_resource["questionnaire_response"] = {
#                 "reference": {"fhir:v": self.ques_response_uri},
#                 "fhir:link": {"@id": self.ques_response_uri}
#             }
        

        # Set the linkId
        if pd.notna(self.questionnaire_item_uri):
            response_item_resource["linkId"] = {"fhir:v": self.questionnaire_item_uri}

        # Set the question text
        if pd.notna(self.question):
            response_item_resource["text"] = {"fhir:v": self.question}

        # Assemble the answer(s)
        answers = []
        if pd.notna(self.value):
            dtype = self.data_type
            value = self.value

            if dtype in ['Text', 'Text memo', 'Signature', 'Drop-down list', 'List of values', 'Search list'] and isinstance(value, str):
                answers.append({"valueString": value})

            elif dtype == 'Boolean':
                if isinstance(value, bool):
                    answers.append({"valueBoolean": value})
                elif str(value).lower() in ['true', 'false']:
                    answers.append({"valueBoolean": bool(value)})

            elif dtype == 'Date' and isinstance(value, str):
                answers.append({"valueDate": {"fhir:v": value}})

            elif dtype == 'Date Time' and isinstance(value, str):
                answers.append({"valueDateTime": {"fhir:v": value}})
                
            elif dtype == 'Integer':
                try:
                    #int_value = int(float(value))  # First cast to float to handle '1.0', then to int
                    int_value = value  # First cast to float to handle '1.0', then to int
                    answers.append({"valueInteger": {"fhir:v": int_value}})
                except ValueError:
                    # Handle or log this situation as per need
                    print(f"Invalid integer value: {value}")


            elif dtype == 'Real':
                try:
                    answers.append({"valueDecimal": {"fhir:v": float(value)}})
                except ValueError:
                    print(f"Failed to cast value {value} to float for datatype {dtype}")

            elif dtype == 'File' and isinstance(value, str):
                answers.append({"valueAttachment": {"url": value, "contentType": "mime/type"}})  # Replace mime/type with the actual mime type if accessible.
            
            else:
                value_with_type = f"{dtype}: {value}"
                answers.append({"valueString": value_with_type})

        if answers:
            response_item_resource["answer"] = answers

        # Formulate nexus_dict
        nexus_dict = {
            "@context": [
                {"@vocab": "http://hl7.org/fhir/"},
                "https://bluebrain.github.io/nexus/contexts/metadata.json",
                {"fhir": "http://hl7.org/fhir/"}
            ],
            "@id": self.response_item_uri,
            "@type": "fhir:QuestionnaireResponse.Item",
            "@reverse": {
                "fhir:QuestionnaireResponse.Item": {
                    "@id": self.ques_response_uri
                }
            }
        }

        nexus_dict.update(response_item_resource)

        return nexus_dict

    def update_to_nexus(self,  nexus_deployment, org, project,token):
        response_item_uri= f"{self.response_item_uri}"
        # URL encode the response_item_uri
        response_item_uri_encoded = quote(response_item_uri, safe='')
        
        resource_url = f"{nexus_deployment}/resources/{org}/{project}/_/{response_item_uri_encoded}"
        
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
                resource_url = f"{nexus_deployment}/resources/{org}/{project}/_"
                create_response = requests.post(resource_url, json=self.fhir_nexus_resource(),  headers=headers)
                create_response.raise_for_status()
                print("Resource successfully created.")
            except requests.exceptions.HTTPError as e:
                print(f"Creation Failed: {e}")

