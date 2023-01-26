import os
import re
import ast
import pandas as pd
from google.cloud import bigquery


def save_file(results, file_path):
    """ Helper function to save/append results"""
    if os.path.exists(file_path) is False:
        results.to_csv(file_path, mode='w', header=False)
    else:
        results.to_csv(file_path, mode='a', header=False)


def chunker(seq, size):
    """ Helper function to chunk a list into parts"""
    return (seq[pos:pos + size] for pos in range(0,
                                                 len(seq),
                                                 size))


def get_data_from_lists(query_list, client, query_type):
    """
    A helper function to get rows of data
    from an input list of ISSNs.

    :param query_list: list of items to query
    :param client: google query client
    :param query_type: object type to query
    :return: a GBC iterator containing all data
    """
    try:
        if query_type == 'issn':
            QUERY = """
                    SELECT id, title.preferred, doi, journal.issn,
                    type, date_normal, category_for,
                    citations_count, research_org_cities,
                    research_org_country_names,
                    altmetrics, reference_ids,
                    citations
                    FROM `dimensions-ai.data_analytics.publications`
                    WHERE type='article' AND journal.issn IN UNNEST(%s)""" %(query_list)
            query_job = client.query(QUERY)
        elif query_type == 'article':
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("pubids", "STRING", query_list),
                ]
            )
            QUERY = """
                    SELECT id, title.preferred, doi, journal.issn,
                    type, date_normal, category_for,
                    citations_count, research_org_cities,
                    research_org_country_names,
                    altmetrics, reference_ids,
                    citations
                    FROM `dimensions-ai.data_analytics.publications` p
                    WHERE type='article' AND p.id IN UNNEST(@pubids)""" %(query_list)
            query_job = client.query(QUERY, job_config=job_config)
        rows = query_job.result()
        return rows
    except Exception as e:
        print(e)


def get_all_data(chunk_size, file_path, client, query_list, query_type):
    """ Helper function to get all data from iterative queries """
    if chunk_size < len(query_list):
        for chunk in chunker(query_list, chunk_size):
            results = get_data_from_lists(chunk,
                                          client,
                                          query_type).to_dataframe()
            save_file(results, file_path)
    elif chunk_size == len(query_list):
        results = get_data_from_lists(query_list,
                                      client,
                                      query_type).to_dataframe()
        save_file(results, file_path)
    else:
        print('Bad Chunksize')


def get_all_refs(df):
    """ Get all references from a pubid dataframe"""
    all_refs = []
    for element in df['reference_ids'].to_list():
        element = element.replace('\n','')
        element = element.replace('[', '')
        element = element.replace(']', '')
        element = element.replace("'", '')
        element = element.split(' ')
        for ele in element:
            if len(ele)>0:
                all_refs.append(ele)
    return list(set(all_refs))


def get_all_citations(df):
    """ Get all citations from a pubid dataframe"""
    all_citations = []
    for element in df['citations'].to_list():
        if len(element) > 2:
            for ele in element.split('\n'):
                x = ast.literal_eval(re.search('({.+})', ele).group(0))
                all_citations.append(x['id'])
    return list(set(all_citations))


def main():
    MY_PROJECT_ID = "dimensionspkp"
    client = bigquery.Client(project=MY_PROJECT_ID)
    raw_path = os.path.join("..", "data", "raw")
    raw_data = pd.read_csv(os.path.join(raw_path,
                                        "ojs_issns_in_dimensions_2020.csv"))
    issns_to_query = raw_data["issn"].tolist()
    print("Total ISSNs to query: ", len(issns_to_query))
    dim_out = os.path.join('..', 'data', 'raw', 'from_dimensions')
    file_name = 'pubs_from_all_issns.csv'
    file_path = os.path.join(dim_out, file_name)
    number_issns = 4200 # hardcoded to circumvent limits
    get_all_data(number_issns, file_path, client, issns_to_query, 'issn')
    all_pubs_from_issn = pd.read_csv(file_path,
                                     usecols=[1, 15, 16],
                                     names=['id',
                                            'reference_ids',
                                            'citations'])
    print('Total number of papers', len(all_pubs_from_issn))
    all_refs = get_all_refs(all_pubs_from_issn)
    print('Total references to query: ', len(all_refs))
    all_citations = get_all_citations(all_pubs_from_issn)
    print('Total citations to query: ', len(all_citations))
    file_name = 'references_of_all_pubs.csv'
    file_path = os.path.join(dim_out, file_name)
    num_pubids = 300000 # hardcoded to circumvent limits
    get_all_data(num_pubids, file_path, client, all_refs, 'article')
    file_name = 'citations_of_all_pubs.csv'
    file_path = os.path.join(dim_out, file_name)
    get_all_data(num_pubids, file_path, client, all_citations, 'article')


if __name__ == '__main__':
    main()