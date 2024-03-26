from dataclasses import dataclass
import datetime
import pandas as pd
from .base import Base
from datetime import date
from urllib.parse import quote 
from urllib.parse import urljoin
import requests

@dataclass
class Encounter(Base):
    patient_uri: str = None
    visit_id: str = None
    subject_status: str = None
    visit_date: date = None  # Date type here
    visit_name: str = None
    centre: str = None
    subject_de_status: str = None
    visit_occ: str = None
    form_occurence: str = None
    encounter_uri: str = None

    def visit_date_isoformat(self):
        if isinstance(self.visit_date, str):
            return datetime.datetime.strptime(self.visit_date, "%Y-%m-%d").date().isoformat()
        elif isinstance(self.visit_date, date):
            return self.visit_date.isoformat()
        return None

    def fhir_nexus_resource(self) -> dict:
        encounter = {}

        # Patient reference
        if self.patient_uri and pd.notna(self.patient_uri):
            encounter["subject"] = {
                "reference": {"fhir:v": self.patient_uri},
                "fhir:link": {"@id": self.patient_uri}
            }

        # Identifier for the encounter (e.g. visit_id)
        if self.visit_id and pd.notna(self.visit_id):
            encounter["identifier"] = [{
                "system": {"fhir:v": "visit_uri"},
                "value": {"fhir:v": self.visit_id}
            }]

        # Status
        if self.subject_status and pd.notna(self.subject_status):
            encounter["status"] = {"fhir:v": self.subject_status}

        # Visit date
        visit_date_str = self.visit_date_isoformat()
        if visit_date_str:
            encounter["period"] = {
                "start": {"fhir:v": visit_date_str}
            }

        # Visit name
        if self.visit_name and pd.notna(self.visit_name):
            encounter["reasonCode"] = [{
                "text": {"fhir:v": self.visit_name}
            }]

        # Extensions
        extensions = []
        if self.centre and pd.notna(self.centre):
            extensions.append({
                "url": {"fhir:v": "custom:centre"},
                "valueString": {"fhir:v": self.centre}
            })
        if self.subject_de_status and pd.notna(self.subject_de_status):
            extensions.append({
                "url": {"fhir:v": "custom:subject_de_status"},
                "valueString": {"fhir:v": self.subject_de_status}
            })
        if self.visit_occ and pd.notna(self.visit_occ):
            extensions.append({
                "url": {"fhir:v": "custom:visit_occ"},
                "valueString": {"fhir:v": self.visit_occ}
            })
        if self.form_occurence and pd.notna(self.form_occurence):
            extensions.append({
                "url": {"fhir:v": "custom:form_occurence"},
                "valueString": {"fhir:v": self.form_occurence}
            })
        if extensions:
            encounter["extension"] = extensions

        # Formulate nexus_dict
        nexus_dict = {
            "@context": [
                {"@vocab": "http://hl7.org/fhir/"},
                "https://bluebrain.github.io/nexus/contexts/metadata.json",
                {"fhir": "http://hl7.org/fhir/"}
            ],
            "@id": self.encounter_uri,
            "@type": "fhir:Encounter",
        }

        nexus_dict.update(encounter)

        return nexus_dict

    def update_to_nexus(self, nexus_url, org, project,token):
        enc_uri= f"{self.encounter_uri}"
        # URL encode the encounter_uri
        enc_uri_encoded = quote(enc_uri, safe='')
        resource_url = f"{nexus_url}/resources/{org}/{project}/_/{enc_uri_encoded}"
        
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