import os
import re
import ast
import traceback
import pandas as pd
from tqdm import tqdm
from google.cloud import bigquery

#article_headers = ['id', 'title.preferred', 'doi', 'journal.issn',
#                   'journal.eissn', 'type', 'date_normal',
#                   'category_for', 'citations_count', 'research_org_cities',
#                   'research_org_country_names', 'altmetrics',
#                   'reference_ids', 'citations']


def load_issns(path, year):
    return pd.read_csv(os.path.join(path, str(year),
                                    f'full_ojs_issn_list_{year}.csv'))


def get_raw_doi_list(raw_path):
    doi_path = os.path.join(raw_path, 'dois')
    doi_file = os.path.join(os.path.join(doi_path, 'ojs-DOIs-2022-12-01.txt'))
    doi_df = pd.read_csv(doi_file, header=None, names=['doi'])
    doi_list = doi_df['doi'].tolist()
    return doi_list


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
                    SELECT id, reference_ids, citations, doi, journal.issn,
                    journal.eissn, type, date_normal, category_for, metrics,
                    research_orgs, researcher_ids, journal, research_org_country_names,
                    altmetrics, title, abstract, concepts, funder_orgs
                    FROM `dimensions-ai.data_analytics.publications`
                    WHERE type='article' AND (journal.issn IN UNNEST(%s) OR journal.eissn IN UNNEST(%s))""" %(query_list, query_list)
            query_job = client.query(QUERY)
        elif query_type == 'article':
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("pubids", "STRING", query_list),
                ]
            )
            QUERY = """
                    SELECT id, reference_ids, citations, doi, journal.issn,
                    journal.eissn, type, date_normal, category_for, metrics,
                    research_orgs, researcher_ids, journal, research_org_country_names,
                    altmetrics, title, abstract, concepts, funder_orgs
                    FROM `dimensions-ai.data_analytics.publications` p
                    WHERE type='article' AND p.id IN UNNEST(@pubids)""" %(query_list)
            query_job = client.query(QUERY, job_config=job_config)
        rows = query_job.result()
        return rows
    except Exception as e:
        print(traceback.format_exc())


def get_all_data(chunk_size, file_path, client, query_list, query_type):
    """ Helper function to get all data from iterative queries"""
    if chunk_size < len(query_list):
        print(f'We have {len(query_list)} {query_type}s with chunksize {chunk_size}')
        for chunk in tqdm(chunker(query_list, chunk_size)):
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
            if len(ele) > 0:
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


#def load_dimensions_returns(path):
#    return pd.read_csv(path,
#                   index_col=0,
#                       names=article_headers,
#                       low_memory=False)


def basic_coverage(issns, returns, year):
    print(f'Length of {year} ISSNs from the OJS: ',
          len(issns))
    unique_issns = len(issns)
    print(f'Length of unique {year} ISSNs from OJS: ',
          unique_issns)
    print(f'Number articles returned from {year} across issns or eissns: ',
          len(returns))
    unique_issn_returns = len(returns['journal.issn'].unique())
    print('Number of unique issns returned: ', unique_issn_returns)
    unique_eissn_returns = len(returns['journal.eissn'].unique())
    print('Number of unique eissns returned: ', unique_eissn_returns)
    returns = returns.drop_duplicates()
    all_issn = list(set(returns['journal.issn'].unique().tolist() +
                        returns['journal.eissn'].unique().tolist()))
    is_in_dim = issns[issns["issn_ojs"].isin(all_issn)]
    print('Number of unique issn+eissns: ', len(all_issn))
    print(round(len(is_in_dim)/len(issns)*100, 2))


def get_refs_and_cites(issn_out, client):
    all_pubs_from_issn = pd.read_csv(os.path.join(issn_out, 'pubs_from_all_issns.csv'),
                                     usecols=[1, 2, 3],
                                     names=['id',
                                            'reference_ids',
                                            'citations'])
    print('Total number of papers', len(all_pubs_from_issn))
    all_refs = get_all_refs(all_pubs_from_issn)
    print('Total references to query: ', len(all_refs))
    all_citations = get_all_citations(all_pubs_from_issn)
    print('Total citations to query: ', len(all_citations))
    file_name = 'references_of_all_pubs.csv'

    file_path = os.path.join(issn_out, file_name)
    num_pubids = 225000 # hardcoded to circumvent limits
    get_all_data(num_pubids, file_path, client, all_refs, 'article')
    file_name = 'citations_of_all_pubs.csv'
    file_path = os.path.join(issn_out, file_name)
    get_all_data(num_pubids, file_path, client, all_citations, 'article')


def main():
    MY_PROJECT_ID = "dimensionsv3"
    year = 2022
    print('Initializing GBQ')
    client = bigquery.Client(project=MY_PROJECT_ID)
    issn_out = os.path.join('..',
                            'data',
                            'raw',
                            'from_dimensions',
                            'issn',
                            str(year))
    doi_out = os.path.join('..',
                           'data',
                           'raw',
                           'from_dimensions',
                           'doi'
                           )
    issn_file_name = 'pubs_from_all_issns.csv'
    dim_issn_out_path = os.path.join(issn_out, issn_file_name)
    number_issns = 3000 # hardcoded to circumvent limits: why needed?
    raw_path = os.path.join("..", "data", "raw")
    print('Loading raw ISSN data')
    raw_issn = load_issns(os.path.join(raw_path,
                                       'issn_inputs'),
                          year)
    issns_to_query = raw_issn["issn_ojs"].tolist()
    # Get all ISSN data here
    if os.path.exists(dim_issn_out_path) is False:
        get_all_data(number_issns,
                     dim_issn_out_path,
                      client,
                      issns_to_query,
                      'issn')
    get_refs_and_cites(issn_out, client)


if __name__ == '__main__':
    main()
