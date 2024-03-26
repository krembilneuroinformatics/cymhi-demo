
from dataclasses import dataclass
import datetime
import pandas as pd
from .base import Base
from datetime import date
from urllib.parse import quote 
from urllib.parse import urljoin
import requests


@dataclass
class Organization(Base):
    org_uri: str = None
    org_id: str = None
    official_name: str = None
    description: str = None
    email: str = None
    website_url: str = None
#    org_type: str = None
    province: str = None


    def fhir_nexus_resource(self) -> dict:
        organization = {}
        
        # Set the name
        if self.official_name and pd.notna(self.official_name):
            organization["name"] = {"fhir:v": self.official_name}
            
        # Set the id
        if self.org_id and pd.notna(self.org_id):
            organization["id"] = {"fhir:v": self.org_id}
            
        # Set the website
        if self.website_url and pd.notna(self.website_url):
            organization["id"] = {"fhir:v": self.website_url}
  
        # Telecom
        telecom_data = []
#         if self.phone and pd.notna(self.phone):
#             telecom_data.append({
#                 "system": {"fhir:v": "phone"},
#                 "value": {"fhir:v": self.phone}
#             })

        if self.email and pd.notna(self.email):
            telecom_data.append({
                "system": {"fhir:v": "email"},
                "value": {"fhir:v": self.email}
            })

#         if self.website_url and pd.notna(self.website_url):
#             telecom_data.append({
#                 "system": {"fhir:v": "website_url"},
#                 "value": {"fhir:v": self.website_url}
#             })
        if telecom_data:
            organization["telecom"] = telecom_data
            
        # address
        address_data = []    
        
        if self.province and pd.notna(self.province):
            address_data.append({
                "system": {"fhir:v": "state"},
                "value": {"fhir:v": self.province}
            })
        
        if address_data:
            organization["address"] = address_data 

         
        # Formulate nexus_dict
        nexus_dict = {
            "@context": [
                {"@vocab": "http://hl7.org/fhir/"},
                "https://bluebrain.github.io/nexus/contexts/metadata.json",
                {"fhir": "http://hl7.org/fhir/"}
            ],
            "@id": self.org_uri,
            "@type": "fhir:Organization"
        }

        nexus_dict.update(organization)

        return nexus_dict

    def update_to_nexus(self, nexus_url, org, project,token):
        org_uri= f"{self.org_uri}"
        # URL encode the org_uri
        org_uri_encoded = quote(org_uri, safe='')
        
        resource_url = f"{nexus_url}/resources/{org}/{project}/_/{org_uri_encoded}"
        
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

