from dataclasses import dataclass
import pandas as pd
from .base import Base
import requests
from urllib.parse import quote 
from urllib.parse import urljoin

@dataclass
class QuestionnaireResponse(Base):
    questionnaire_uri: str = None
    patient_uri: str = None
    encounter_uri: str = None
    ques_response_uri: str = None

    def fhir_nexus_resource(self) -> dict:
        questionnaire_response = {}

        # Set questionnaire reference
        if self.questionnaire_uri and pd.notna(self.questionnaire_uri):
            questionnaire_response["questionnaire"] = {
                "reference": {"fhir:v": self.questionnaire_uri},
                "fhir:link":{"@id": self.questionnaire_uri}
            }

        # Set patient reference
        if self.patient_uri and pd.notna(self.patient_uri):
            questionnaire_response["subject"] = {
                "reference": {"fhir:v": self.patient_uri},
                "fhir:link":{"@id":self.patient_uri }
            }

        # Set encounter reference
        if self.encounter_uri and pd.notna(self.encounter_uri):
            questionnaire_response["context"] = {
                "reference": {"fhir:v": self.encounter_uri},
                "fhir:link":{"@id":self.encounter_uri }
            }

        # Formulate nexus_dict
        nexus_dict = {
            "@context": [
                {"@vocab": "http://hl7.org/fhir/"},
                "https://bluebrain.github.io/nexus/contexts/metadata.json",
                {"fhir": "http://hl7.org/fhir/"}
            ],
            "@id": self.ques_response_uri,
            "@type": "fhir:QuestionnaireResponse"
        }

        nexus_dict.update(questionnaire_response)

        return nexus_dict

    def update_to_nexus(self, nexus_url, org, project,token):
        ques_response_uri= f"{self.ques_response_uri}"
        # URL encode the ques_response_uri
        ques_response_uri_encoded = quote(ques_response_uri, safe='')
        
        resource_url = f"{nexus_url}/resources/{org}/{project}/_/{ques_response_uri_encoded}"
        
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