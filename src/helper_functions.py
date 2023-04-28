import pandas as pd
import os

article_headers = ['id', 'title.preferred', 'doi', 'journal.issn', 'journal.eissn',
                   'type', 'date_normal', 'category_for',
                   'citations_count', 'research_org_cities',
                   'research_org_country_names',
                   'altmetrics', 'reference_ids',
                   'citations']


def build_spine(issn_inputs_2021, issn_to_issn_L, issn_l_to_issn):
    merged_spine = pd.merge(issn_inputs_2021, issn_to_issn_L,
                            how='left',
                            left_on='issn_ojs',
                            right_on='ISSN')
    merged_spine = pd.merge(merged_spine,
                           issn_l_to_issn,
                           how='left',
                           left_on='ISSN-L',
                           right_on = 'ISSN-L')
    merged_spine = merged_spine.drop('ISSN', axis=1)
    merged_spine_path = os.path.join(os.getcwd(),
                                     '..',
                                     'data',
                                     'merged_spine',
                                     'merged_OJS_spine.csv')
    number_null = len(merged_spine[~merged_spine['ISSN-L'].notnull()])
    print(f'Number of unmerged ISSNs: {number_null}')
    unique_issn_l = merged_spine['ISSN-L'].unique()
    print(f'Number of unique ISSN-Ls: {len(unique_issn_l)}')
    merged_spine.to_csv(merged_spine_path, index=False)


def prepare_issn_l():
    issn_l_path = os.path.join(os.getcwd(), '..', 'data', 'issn_l_lookup')
    issn_to_issn_l = os.path.join(issn_l_path, '20230427.ISSN-to-ISSN-L.txt')
    issn_to_issn_l = pd.read_csv(issn_to_issn_l, sep='\t', index_col=False)
    issn_l_to_issn_filepath = os.path.join(issn_l_path,
                                           '20230427.ISSN-L-to-ISSN.txt')
    index = pd.read_csv(issn_l_to_issn_filepath, sep='\t',
                        usecols = ['ISSN-L'], index_col=False)
    issn_l_to_issn = pd.DataFrame(index = index['ISSN-L'],
                                  columns = ['All ISSN'])
    file1 = open(issn_l_to_issn_filepath, 'r')
    Lines = file1.readlines()
    count = 0
    for line in Lines:
        count += 1
        if count>1:
            split_line = line.strip().split('\t')
            issn_l = split_line[0]
            issns = split_line[1:]
            issn_l_to_issn.at[issn_l, 'All ISSN'] = issns
    issn_l_to_issn = issn_l_to_issn.reset_index()
    return issn_to_issn_l, issn_l_to_issn



def basic_coverage(issns, returns, year):
    print('Length of ISSNs in the raw file: ',
          len(issns))
    unique_issns = len(issns['issn_ojs'].unique())
    print(f'Length of unique {year} ISSNs from Saurabh: ',
          unique_issns)
    print(f'Number of articles returned from Dimensions {year} across all p-issns or e-issns: ',
          len(returns))
    unique_issn_returns = len(returns['journal.issn'].unique())
    print('Number of unique issns returned: ', unique_issn_returns)
    unique_eissn_returns = len(returns['journal.eissn'].unique())
    print('Number of unique eissns returned: ', unique_eissn_returns)
    all_issn = list(set(returns['journal.issn'].unique().tolist() +
                        returns['journal.eissn'].unique().tolist()))
    print('Number of unique issn+eissns: ', len(all_issn))
    print(100*len(all_issn)/unique_issns)


def load_issns(path, year):
    return pd.read_csv(os.path.join(path, str(year),
                                    f'full_ojs_issn_list_{year}.csv'))


def load_dimensions_returns(path, year):
    return pd.read_csv(os.path.join(path, year,
                                    'pubs_from_all_issns.csv'),
                       index_col=0,
                       names=article_headers,
                       low_memory=False
                      )