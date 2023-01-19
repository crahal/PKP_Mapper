def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def pub_ids_from_issns(issn_subset, client):
    try:
        QUERY = """
        SELECT id, title.preferred, doi, journal.issn,
        publisher, type, date, open_access_categories,
        category_for, citations_count, journal, researcher_ids,
        research_orgs, research_org_cities, research_org_city_names,
        research_org_countries, research_org_country_names, concepts,
        altmetrics
        FROM `dimensions-ai.data_analytics.publications`
        WHERE journal.issn IN UNNEST(%s)""" %(issn_subset)
        query_job = client.query(QUERY)  # API request
        rows = query_job.result()  # Wait for query to finish
        return rows
    except Exception as e:
        print(e)