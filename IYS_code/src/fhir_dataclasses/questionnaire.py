
from dataclasses import dataclass
import datetime
import pandas as pd
from .base import Base
from datetime import date
from urllib.parse import quote 
from urllib.parse import urljoin
import requests

@dataclass
class Questionnaire(Base):
    org_uri: str = None
    questionnaire_name: str = None
    questionnaire_uri: str = None

    def fhir_nexus_resource(self) -> dict:
        questionnaire = {}
        
        # Set managing organization
        if self.org_uri and pd.notna(self.org_uri):
            questionnaire["managingOrganization"] = {
                "reference": {"fhir:v": self.org_uri},
                "fhir:link": {"@id": self.org_uri}
            }

        # Set the name
        if self.questionnaire_name and pd.notna(self.questionnaire_name):
            questionnaire["name"] = {"fhir:v": self.questionnaire_name}

         
        # Formulate nexus_dict
        nexus_dict = {
            "@context": [
                {"@vocab": "http://hl7.org/fhir/"},
                "https://bluebrain.github.io/nexus/contexts/metadata.json",
                {"fhir": "http://hl7.org/fhir/"}
            ],
            "@id": self.questionnaire_uri,
            "@type": "fhir:Questionnaire"
        }

        nexus_dict.update(questionnaire)

        return nexus_dict

    def update_to_nexus(self, nexus_url, org, project,token):
        ques_uri= f"{self.questionnaire_uri}"
        # URL encode the ques_uri
        ques_uri_encoded = quote(ques_uri, safe='')
        
        resource_url = f"{nexus_url}/resources/{org}/{project}/_/{ques_uri_encoded}"
        
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

