import os
import csv
import time
import requests

import numpy as np
import pandas as pd
from tqdm import tqdm
from loguru import logger
from ratelimit import limits
from requests.exceptions import JSONDecodeError


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


def get_log_filename():
    return time.strftime("logs_%S_%M_%H_%d_%m_%Y.log")


# Define a function to generate CSV file path with timestamp
def get_csv_filename():
    timestamp = time.strftime("%S%M%H%d%m%Y")
    return os.path.join(os.getcwd(),
                        '..',
                        'data',
                        'scopus_counts',
                        f'scopus_counts_{timestamp}.csv')


def main():
    log_file = os.path.join(os.getcwd(), '..', 'logging', get_log_filename())
    logger.add(log_file)
    keys_path = os.path.join(os.getcwd(), '..', 'keys')
    keys = make_apikey_list(keys_path)

    raw_path = os.path.join("..", "data", "raw")
    year = 2022
    logger.info('Loading raw ISSN data')
    raw_issn = load_issns(os.path.join(raw_path,
                                       'issn_inputs'),
                          year)
    issn_list = raw_issn['issn_ojs'].tolist()
    csv_file_path = get_csv_filename()
    csv_header = ['raw_issn', 'search_issn_count', 'search_eissn_count',
                  'serial_prism:issn', 'serial_prism:eIssn', 'serial_dc:title']
    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(csv_header)
    key_counter = 0
    base_url = 'http://api.elsevier.com/content/'
    for issn in tqdm(issn_list):
        url_search = base_url + f'search/scopus?query=issn({issn})'
        url_esearch = base_url + f'search/scopus?query=eissn({issn})'
        url_title = base_url + f'serial/title/issn/{issn}'
        returned = False
        while returned is False:
            api_return_search = call_scopus_search_api(url_search, keys[key_counter])
            api_return_esearch = call_scopus_search_api(url_esearch, keys[key_counter])
            api_return_title = call_scopus_search_api(url_title, keys[key_counter])
            search_remaining = int(api_return_search.headers.get('X-RateLimit-Remaining'))
            title_remaining = int(api_return_title.headers.get('X-RateLimit-Remaining'))
            if (search_remaining==0) or (title_remaining==0):
                key_counter += 1
                logger.info(f'No keys left. Restarting while condition.')
            else:
                logger.info(f'We have {search_remaining} and {title_remaining} calls remaining for key: {keys[key_counter]}')
                if ((int(api_return_search.status_code == 200) | (int(api_return_search.status_code == 404))) and \
                        (int(api_return_esearch.status_code == 200) | (int(api_return_esearch.status_code == 404))) and \
                        ((int(api_return_title.status_code == 200) | (int(api_return_title.status_code == 404))))):
                    logger.warning(f"API status codes: {api_return_search.status_code}, {api_return_title.status_code}")
                    logger.info(f'Successful API request for {issn}.')
                    returned = True
                else:
                    logger.warning(f'Problem with API call for {issn}.')
                    logger.warning(f"API status codes: {api_return_search.status_code},"
                                   f"{api_return_esearch.status_code}"
                                   f"{api_return_title.status_code}")
                    logger.warning(f'Key counter is: {key_counter}')
                    logger.warning(f'Having a sleep for a few seconds')
                    time.sleep(20)
        try:
            if 'service-error' in api_return_title.json():
                if api_return_title.json()['service-error']['status']['statusCode'] == 'RESOURCE_NOT_FOUND':
                    prism_issn = 'Resource Not Found'
                    prism_eissn = 'Resource Not Found'
                    dc_title = 'Resource Not Found'
                    logger.warning(f'Resource not found for {issn}.')
                else:
                    prism_issn = 'Other Service Error'
                    prism_eissn = 'Other Service Error'
                    dc_title = 'Other Service Error'
                    logger.warning(f'Other service error for {issn}.')
            else:
                results_title = api_return_title.json()['serial-metadata-response']['entry']
                if len(results_title) == 1:
                    try:
                        prism_issn = results_title[0]['prism:issn']
                    except KeyError:
                        prism_issn = 'No prism:issn data'
                        logger.warning(f'No prims:issn data for {issn}.')
                    try:
                        prism_eissn = results_title[0]['prism:eIssn']
                    except KeyError:
                        prism_eissn = 'No prims:eissn data'
                        logger.warning(f'No prims:eissn data for {issn}.')
                    try:
                        dc_title = results_title[0]['dc:title']
                    except KeyError:
                        dc_title = 'No dc:title data'
                        logger.warning(f'No dc:title data for {issn}.')
                else:
                    prism_issn = 'More than 1 return'
                    prism_eissn = 'More than 1 return'
                    dc_title = 'More than 1 return'
                    logger.warning(f'More than one return for {issn}.')
        except (KeyError, JSONDecodeError, requests.exceptions.RequestException):
            prism_issn = 'Malformed json return'
            prism_eissn = 'Malformed json return'
            dc_title = 'Malformed json return'
            logger.warning(f'Malformed JSON for {issn}.')
        try:
            if 'search-results' in api_return_search.json().keys():
                results_search = api_return_search.json()['search-results']
                issn_count = int(results_search['opensearch:totalResults'])
            else:
                issn_count = np.nan
                logger.warning(f'No search results for {issn}.')
        except (KeyError, JSONDecodeError, requests.exceptions.RequestException):
            issn_count = np.nan
            logger.warning(f'Errors for search-results for {issn}.')
        try:
            if 'search-results' in api_return_esearch.json().keys():
                results_esearch = api_return_esearch.json()['search-results']
                eissn_count = int(results_esearch['opensearch:totalResults'])
            else:
                eissn_count = np.nan
                logger.warning(f'No esearch results for {issn}.')
        except (KeyError, JSONDecodeError, requests.exceptions.RequestException):
            eissn_count = np.nan
            logger.warning(f'Errors for esearch-results for {issn}.')
        with open(csv_file_path, 'a', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([issn,
                                 issn_count,
                                 eissn_count,
                                 prism_issn,
                                 prism_eissn,
                                 dc_title])


if __name__ == '__main__':
    main()
