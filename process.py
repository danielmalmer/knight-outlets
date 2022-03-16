import pandas as pd


def normalize_keys(key):
    the_regex = '^(the|\\(the\\)) '
    thing = key.str.lower()\
        .replace(the_regex, '', regex=True)\
        .replace('[- ]', ' ', regex=True)
    return thing


def create_codebook():

    codebook = pd.read_csv('codebook.csv')

    return pd.DataFrame(
        data={'code': codebook['pass_2'].values, },
        index=codebook['outlet'].str.lower()
    )


# Starts with scores that aren't found in PCAD data.
# Then, add scores indexed by source name, then by URL.
def create_scorebook():

    scores = pd.read_csv('scores.csv')
    pcad = pd.read_csv('pcad-digest.csv')

    scores = pd.Series(
        data=scores['score'].values,
        index=normalize_keys(scores['code']).values
    )

    source_scores = pd.Series(
        data=pcad['bias_score'].values,
        index=normalize_keys(pcad['source'])
    )

    domain_scores = pd.Series(
        data=pcad['bias_score'].values,
        index=normalize_keys(pcad['source_domain'])
    )

    # import pdb; pdb.set_trace()

    scores = scores.combine_first(source_scores.combine_first(domain_scores))

    return scores


def add_codes(outlets, codebook):

    columns = (
        ('34_a', 'code_a', ),
        ('34_b', 'code_b', ),
        ('34_c', 'code_c', ),
    )

    for outlet_col, code_col in columns:

        df = pd.DataFrame(
            data={
                'outlet': outlets[outlet_col].str.lower(),
            }
        ).merge(codebook, on='outlet', how='left')

        outlets[code_col] = df['code']


def add_scores(outlets, scorebook):

    columns = (
        ('code_a', 'score_a', ),
        ('code_b', 'score_b', ),
        ('code_c', 'score_c', ),
    )

    for code_col, score_col in columns:

        df = pd.DataFrame(
            data={
                'code': normalize_keys(outlets[code_col]),
            }
        ).merge(
            scorebook.rename('score'),
            left_on='code',
            right_index=True,
            how='left'
        )

        outlets[score_col] = df['score']


def write_summary(outlets, scorebook):

    all_codes = pd.concat(
        [
            outlets['code_a'],
            outlets['code_b'],
            outlets['code_c'],
        ]
    )

    counts = all_codes.value_counts()

    summary = pd.DataFrame(
        data={
            'code': counts.keys(),
            'count': counts.values
        }
    )

    scores = pd.DataFrame(
        data={
            'code': normalize_keys(summary['code']),
        }
    ).merge(
        scorebook.rename('score'),
        left_on='code',
        right_index=True,
        how='left'
    )

    summary['score'] = scores['score']

    summary.to_csv(
        'summary.csv',
        columns=['code', 'count', 'score'],
        index=False
    )


def main():

    outlets = pd.read_csv('outlets_opencode.csv')
    scorebook = create_scorebook()
    codebook = create_codebook()

    # Adds code_a, code_b, and code_c columns containing outlet codes.
    add_codes(outlets, codebook)

    # Adds score_a, score_b, and score_c columns containing bias scores.
    add_scores(outlets, scorebook)

    outlets.to_csv('outlets-out.csv')

    # Writes file containing counts of outlet codes along with bias scores.
    write_summary(outlets, scorebook)


if __name__ == '__main__':
    main()