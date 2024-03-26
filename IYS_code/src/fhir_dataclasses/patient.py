from dataclasses import dataclass
import datetime
import pandas as pd
from .base import Base 
from datetime import date
import requests
from urllib.parse import quote 
from urllib.parse import urljoin

@dataclass
class Patient(Base):
    org_uri: str = None
    patient_uri: str = None
    age_at_reg: int = None 
    dob: date = None
        
    def dob_isoformat(self):
        if self.dob is None or pd.isna(self.dob) or self.dob == 'NaN' or self.dob == 'NA':
            return None
        dobstr = str(self.dob) 
        try:
            dob_date = datetime.datetime.strptime(dobstr, "%Y-%m-%d").date()
            return dob_date.isoformat()
        except ValueError:
            return None
#         elif isinstance(self.dob, date):
#             return self.dob.isoformat()
#         return None

    def fhir_nexus_resource(self) -> dict:
        patient = {}

        # Date of birth
        dob_str = self.dob_isoformat()
        if dob_str and pd.notna(self.dob):
            patient["birthDate"] = {"fhir:v": dob_str}


        # Managing organization
        if self.org_uri:
            patient["managingOrganization"] = {
                "reference": {"fhir:v": self.org_uri},
                "fhir:link": {"@id": self.org_uri}
            }
       
        # Age at Reg
        if self.age_at_reg:
            patient["age_at_reg"] = {"fhir:v": self.age_at_reg}
            

        
        
           # Extensions
        extensions = []

          
#         # Handle age_at_reg as extension
#         if self.age_at_reg:
#             extensions.append({
#                 "url": {"fhir:v":"https://kcniconfluence.camh.ca/display/ICYF/Documentation+for+FHIR+extensions"},
#                 "valueInteger": {"fhir:v": self.age_at_reg}    
#             })
        
#         if extensions:
#             patient["extension"] = extensions
        
        # Formulate nexus_dict
        nexus_dict = {
            "@context": [
                {"@vocab": "http://hl7.org/fhir/"},
                "https://bluebrain.github.io/nexus/contexts/metadata.json",
                {"fhir": "http://hl7.org/fhir/"}
            ],
            "@id": self.patient_uri,
            "@type": "fhir:Patient",
        }

        nexus_dict.update(patient)

        return nexus_dict              
                
 
    def update_to_nexus(self, nexus_url, org, project,token):
        pat_uri= f"{self.patient_uri}"
        # URL encode the patient_uri
        pat_uri_encoded = quote(pat_uri, safe='')
        
        resource_url = f"{nexus_url}/resources/{org}/{project}/_/{pat_uri_encoded}"
        
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
