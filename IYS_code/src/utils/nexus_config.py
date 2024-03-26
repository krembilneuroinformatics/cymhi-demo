import json
from urllib.request import urlopen
import nexussdk as nexus
from SPARQLWrapper import SPARQLWrapper, POST, JSON
from src.utils import NexusSparqlQuery as qns  # Replace with the actual module name

def initialize_nexus_connection(nexus_deployment, org, project, token):
    # Setting up the SPARQL client
    sparqlview_endpoint = f"{nexus_deployment}/views/{org}/{project}/graph/sparql"
    sparqlview_wrapper = qns.create_sparql_client(sparql_endpoint=sparqlview_endpoint, token=token, http_query_method=POST, result_format=JSON)

    # Create Nexus connection
    nexus.config.set_environment(nexus_deployment)
    nexus.config.set_token(token)
    nexus.permissions.fetch()

    # Import the context needed for Nexus
    url = "https://bluebrainnexus.io/contexts/metadata.json"
    response = urlopen(url)
    metadata_dict = json.loads(response.read())

    return sparqlview_wrapper, metadata_dict

def get_nexus_uri_base(org):
    return f"https://nexus.camh.ca/v1/resources/{org}/{org.lower()}-fhir-data/fhir/"



# def get_nexus_uri_base(org,project):
#     return f"https://nexus-research.camh.ca/v1/resources/{org}/{project}/"