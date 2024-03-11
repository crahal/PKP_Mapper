import os
import csv
import requests
import numpy as np
import pandas as pd
from ratelimit import limits
from tqdm import tqdm


@limits(calls=8, period=1)  # max of 8 requests per 1 second
def call_scopus_search_api(url, key):
    """

    Parameters
    ----------
    url : str
        url to request content from
    key: str
        api key
    Returns
    -------
   response object as returned by requests.get()

    """
    api_return = requests.get(url,
                              headers={'Accept': 'application/json',
                                       'X-ELS-APIKey': key})
    return api_return


def make_apikey_list(keys_path):
    """
    Function to extract list of API keys from files where they
    have been saved in keys directory.

    Parameters
    ----------
    keys_path : str
        path to a directory containing either text files containing API keys
        or subdirectories containing files with API keys or a mix of
        both. Function uses os.walk to extract contents of all files in
        the keys_path directory and all files in all subdirectories of that
        directory. From each file it reaches, it extracts the text
        content and appends it to the returned list.

    Returns
    -------
    a list of strings representing API keys

    """
    apikey_list = []
    for subdir, dirs, files in os.walk(keys_path):
        for filename in files:
            if filename.startswith('elsevier_apikey_'):
                key = os.path.join(os.path.join(subdir, filename))
                with open(key, 'r') as file:
                    apikey_list.append(str(file.readline()).strip())
        if len(apikey_list) > 0:
            print(f'Got {len(apikey_list)} keys!')
        else:
            print('No api keys!?')
    return apikey_list


def load_issns(path, year):
    return pd.read_csv(os.path.join(path, str(year),
                                    f'full_ojs_issn_list_{year}.csv'))


def main():
    keys_path = os.path.join(os.getcwd(), '..', 'keys')
    keys = make_apikey_list(keys_path)

    raw_path = os.path.join("..", "data", "raw")
    year = 2022
    print('Loading raw ISSN data')
    raw_issn = load_issns(os.path.join(raw_path,
                                       'issn_inputs'),
                          year)
    issn_list = raw_issn['issn_ojs'].tolist()
    csv_file_path = os.path.join(os.getcwd(),
                                 '..',
                                 'data',
                                 'scopus_counts',
                                 'scopus_counts_11032024.csv')
    csv_header = ['raw_issn', 'scopus_count', 'prism:issn', 'prism:eIssn']
    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(csv_header)
    key_counter = 0
    base_url = 'http://api.elsevier.com/content/'
    for issn in tqdm(issn_list):
        url_search = base_url + f'search/scopus?query=issn({issn})'
        url_title = base_url + f'serial/title/issn/{issn}'
        api_return_search = call_scopus_search_api(url_search, keys[key_counter])
        api_return_title = call_scopus_search_api(url_title, keys[key_counter])
        results_search = api_return_search.json()['search-results']
        if 'service-error' in api_return_title.json():
            if api_return_title.json()['service-error']['status']['statusCode'] == 'RESOURCE_NOT_FOUND':
                prism_issn = 'Resource Not Found'
                prism_eissn = 'Resource Not Found'
                dc_title = 'Resource Not Found'
            else:
                prism_issn = 'Other Service Error'
                prism_eissn = 'Other Service Error'
                dc_title = 'Other Service Error'
        else:
            results_title = api_return_title.json()['serial-metadata-response']['entry']
            if len(results_title) == 1:
                try:
                    prism_issn = results_title[0]['prism:issn']
                except KeyError:
                    prism_issn = 'No prism:issn data'
                try:
                    prism_eissn = results_title[0]['prism:eIssn']
                except KeyError:
                    prism_eissn = 'No prism:eIssn data'
                try:
                    dc_title = results_title[0]['dc:title']
                except KeyError:
                    dc_title = 'No dc:title data'
            else:
                prism_issn = 'More than 1 return'
                prism_eissn = 'More than 1 return'
                dc_title = 'More than 1 return'
        try:
            article_count = int(results_search['opensearch:totalResults'])
        except KeyError:
            article_count = np.nan
        if (api_return_search.headers.get('X-RateLimit-Remaining') == 0) or \
                (api_return_title.headers.get('X-RateLimit-Remaining') == 0):
            key_counter += 1
        with open(csv_file_path, 'a', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([issn, article_count, prism_issn, prism_eissn, dc_title])


if __name__ == '__main__':
    main()
