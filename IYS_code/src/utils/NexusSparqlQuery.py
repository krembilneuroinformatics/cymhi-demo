import rdflib as rdflib
import SPARQLWrapper

# Utility functions to create sparql wrapper around a sparql endpoint

from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET, POSTDIRECTLY, CSV
import requests


def create_sparql_client(sparql_endpoint, http_query_method=POST, result_format= JSON, token=None):
    sparql_client = SPARQLWrapper(sparql_endpoint)
    #sparql_client.addCustomHttpHeader("Content-Type", "application/sparql-query")
    if token:
        sparql_client.addCustomHttpHeader("Authorization","Bearer {}".format(token))
    sparql_client.setMethod(http_query_method)
    sparql_client.setReturnFormat(result_format)
    if http_query_method == POST:
        sparql_client.setRequestMethod(POSTDIRECTLY)
    
    return sparql_client

# Utility functions
import pandas as pd

#pd.set_option('display.max_colwidth', None)

# Convert SPARQL results into a Pandas data frame
def sparql2dataframe(json_sparql_results):
    cols = json_sparql_results['head']['vars']
    out = []
    for row in json_sparql_results['results']['bindings']:
        item = []
        for c in cols:
            item.append(row.get(c, {}).get('value'))
        out.append(item)
    return pd.DataFrame(out, columns=cols)

# Send a query using a sparql wrapper 
def query_sparql(query, sparql_client):
    sparql_client.setQuery(query)
    
    result_object = sparql_client.query()
    if sparql_client.returnFormat == JSON:
        return result_object._convertJSON()
    return result_object.convert()