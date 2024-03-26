
from dataclasses import dataclass
import datetime
import pandas as pd
from .base import Base
from datetime import date
from urllib.parse import quote 
from urllib.parse import urljoin
import requests

@dataclass
class QuestionnaireItem(Base):
    item_code: str = None
    question_text: str = None
    data_type: str = None
    answer_options_list: int = None  # This is treated as an integer field
    statistical_name: str = None
    questionnaire_item_uri: str = None
    questionnaire_uri: str = None  # For @reverse field

    def fhir_nexus_resource(self) -> dict:
        questionnaire_item_resource = {}
        
        
#         # Questionnaire reference (this needs to be updated, done to create relation in Nexus)
#         if self.questionnaire_uri and pd.notna(self.questionnaire_uri):
#             questionnaire_item_resource["questionnaire_name"] = {
#                 "reference": {"fhir:v": self.questionnaire_uri},
#                 "fhir:link": {"@id": self.questionnaire_uri}
#             }
        
        

        # Set the linkId
        if self.item_code and pd.notna(self.item_code):
            questionnaire_item_resource["linkId"] = {"fhir:v": self.item_code}

        # Set the text
        if self.question_text and pd.notna(self.question_text):
            questionnaire_item_resource["text"] = {"fhir:v": self.question_text}

        # Set the type
        if self.data_type and pd.notna(self.data_type):
            questionnaire_item_resource["type"] = {"fhir:v": self.data_type}

        # Check if answer_options_list is not None and not -1 
        has_external_answer_option = self.answer_options_list is not None and self.answer_options_list != -1

        # Add the hasExternalAnswerOptionList extension
        questionnaire_item_resource["extension"] = [
            {
                "url": {"fhir:v":"https://kcniconfluence.camh.ca/display/ICYF/hasAnswerOptionList"},
                "valueBoolean": {"fhir:v": has_external_answer_option}
            }
        ]

        # ID for the answerOption list, adding it as another extension
        if has_external_answer_option:
            questionnaire_item_resource["extension"].append({
                "url": {"fhir:v":"https://kcniconfluence.camh.ca/display/ICYF/AnswerOptionListID"},
                "valueString": {"fhir:v": str(self.answer_options_list)}
            })

        # Set the statistical name as an extension
        if self.statistical_name and pd.notna(self.statistical_name):
            questionnaire_item_resource["extension"].append({
                "url": {"fhir:v":"https://kcniconfluence.camh.ca/display/ICYF/statistical_name"},
                "valueString": {"fhir:v": self.statistical_name}
            })

        # Formulate nexus_dict
        nexus_dict = {
            "@context": [
                {"@vocab": "http://hl7.org/fhir/"},
                "https://bluebrain.github.io/nexus/contexts/metadata.json",
                {"fhir": "http://hl7.org/fhir/"}
            ],
            "@id": self.questionnaire_item_uri,
            "@type": "fhir:Questionnaire.Item",
            "@reverse": {
                "fhir:Questionnaire": {
                    "@id":  self.questionnaire_uri
                }
            }
        }

        nexus_dict.update(questionnaire_item_resource)

        return nexus_dict

    def update_to_nexus(self, nexus_url, org, project,token):
        ques_item_uri= f"{self.questionnaire_item_uri}"
        # URL encode the ques_item_uri
        ques_item_uri_encoded = quote(ques_item_uri, safe='')
        
        resource_url = f"{nexus_url}/resources/{org}/{project}/_/{ques_item_uri_encoded}"
        
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


