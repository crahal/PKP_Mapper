import tqdm.notebook as tqdm
import os

def chunker(seq, size):
    """ Helper function to chunk a list into parts"""
    return (seq[pos:pos + size] for pos in range(0,
                                                 len(seq),
                                                 size))


def pub_from_issn_lists(issn_list, client):
    """
    A helper function to get rows of data
    from an input list of ISSNs.

    input: issn_list -- list of issns to query
           client: Google BigQuery
    output: a GBC iterator containing all data
    """
    try:
        QUERY = """
        SELECT id, title.preferred, doi, journal.issn,
        publisher, type, date, open_access_categories,
        category_for, citations_count, journal,
        researcher_ids, research_orgs, research_org_cities,
        research_org_city_names, research_org_countries,
        research_org_country_names, concepts,
        altmetrics
        FROM `dimensions-ai.data_analytics.publications`
        WHERE journal.issn IN UNNEST(%s)""" %(issn_list)
        query_job = client.query(QUERY)  # API request
        rows = query_job.result()  # Wait for query to finish
        return rows
    except Exception as e:
        print(e)


def get_pubs_all_issn(chunk_size, file_path, client, issns_to_query):
    # @TODO a better tqdm decorator...
    for issn_chunk in tqdm(chunker(issns_to_query, chunk_size)):
        results = pub_from_issn_lists(issn_chunk, client).to_dataframe()
        if os.path.exists(file_path) is False:
            results.to_csv(file_path, mode='w', header=False)
        else:
            results.to_csv(file_path, mode='a', header=False)