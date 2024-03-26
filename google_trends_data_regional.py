import os
from collections import namedtuple
from decimal import Decimal
import pandas as pd
from pytrends.request import TrendReq
CATEGORY_MAP = {
    'Advertising & Marketing': 25,
    'Agriculture & Forestry': 46,
    'All categories': 0,
    'Antiques & Collectibles': 64,
    'Apparel': 68,
    'Arts & Entertainment': 3,
    'Autos & Vehicles': 47,
    'Banking': 37,
    'Beauty & Fitness': 44,
    'Books & Literature': 22,
    'Business & Industrial': 12,
    'Card Games': 39,
    'Charity & Philanthropy': 57,
    'Classifieds': 61,
    'Computer & Video Games': 41,
    'Computer Hardware': 30,
    'Computers & Electronics': 5,
    'Construction & Maintenance': 48,
    'Consumer Advocacy & Protection': 97,
    'Consumer Electronics': 78,
    'Consumer Resources': 69,
    'Dating & Personals': 55,
    'Education': 74,
    'Enterprise Technology': 77,
    'Environmental Issues': 82,
    'Ethnic & Identity Groups': 56,
    'Fashion Designers & Collections': 98,
    'Finance': 7,
    'Fitness': 94,
    'Food & Drink': 71,
    'Games': 8,
    'Gifts': 99,
    'Gifts & Special Event Items': 70,
    'Government': 76,
    'Health': 45,
    'Hobbies & Leisure': 65,
    'Home & Garden': 11,
    'Insurance': 38,
    'Internet & Telecom': 13,
    'Jazz': 42,
    'Jobs': 60,
    'Law & Government': 19,
    'Legal': 75,
    'Manufacturing': 49,
    'Marketing Services': 83,
    'Mass Merchants & Department Stores': 73,
    'Movies': 34,
    'Music & Audio': 35,
    'News': 16,
    'Offbeat': 33,
    'Office Services': 28,
    'Office Supplies': 95,
    'Online Goodies': 43,
    'Parenting': 58,
    'People & Society': 14,
    'Performing Arts': 23,
    'Pets & Animals': 66,
    'Programming': 31,
    'Real Estate': 29,
    'Real Estate Agencies': 96,
    'Religion & Belief': 59,
    'Search Engine Optimization & Marketing': 84,
    'Shopping': 18,
    'Skin & Nail Care': 93,
    'Social Issues & Advocacy': 54,
    'Software': 32,
    'Sports': 20,
    'Stereo Systems & Components': 91,
    'TV & Video': 36,
    'Transportation & Logistics': 50,
    'Travel': 67,
    'Vehicle Parts & Accessories': 89,
    'Visual Art & Design': 24,
    'Weather': 63,
    'Web Hosting & Domain Registration': 53,
}

def __escape_single_quote(s):
    """Adds backslash before the single quotes in the string so that they are
    escaped in further string construction.
    Args:
        s (str): Input string.
    Returns:
        str: -
    """
    return s.replace('\'', '')


def get_regional_data(engine):
    # connect to sf
    df = cnx.execute(f'''
            SELECT *
            FROM "_LOOKUP_RETAILER_ENTITY_MAPPING"; 
        ''').fetchall()
    search_again = []
    err_400 = []
    data = []
    df = df[df["ENCODED_TERM"].notnull()]
    geo_by_prov = [
        'CA-ON', 'CA-BC', 'CA-MB', 'CA-NS', 'CA-AB', 'CA-QC', 'CA-NB', 'CA-NL',
        'CA-SK', 'CA-NT', 'CA-YT', 'CA-NU', 'CA-PE'
    ]

    for idx, retailer in enumerate(df.to_dict(orient='records')):
        pytrends = TrendReq(
            hl='en-CA',
            tz=300,
            timeout=(20, 50),
            backoff_factor=0.5,
            retries=100)
        for prov in geo_by_prov:
            try:
                pytrends.build_payload(
                    [__escape_single_quote(retailer["ENCODED_TERM"])],
                    cat=CATEGORY_MAP['Shopping'],
                    timeframe='today 5-y',
                    geo=prov,
                    gprop='')
            except:
                retailer['PROVINCE'] = prov
                err_400.append(retailer)
                continue
            try:
                trend_df = pytrends.interest_over_time().rename(
                    columns={retailer["ENCODED_TERM"]: 'VALUE'})
                trend_df['DATE'] = trend_df.index
                trend_df["SEARCH_TERM"] = retailer["KEYWORD"]
                trend_df["KEYWORD"] = retailer["ENCODED_TERM"]
                trend_df["GEO"] = prov
            except:
                search_again.append(retailer)
                continue
            data_trend = trend_df.to_dict(orient='records')
            data = data + data_trend
    df_trend = pd.DataFrame(data)
    df_trend["GOOGLE_TRENDS_CAT"] = "Shopping"
    df_trend['TIMEFRAME'] = 'today 5-y'
    df_trend["SET_ID"] = 4
    df_trend["ANCHOR_VALUE"] = ''
    df_trend["ANCHOR"] = ''
    df_trend["GPROP"] = ''
    df_trend.to_csv(
        os.path.join("f_web_data", "google_trends", f'gtrend.csv'),
        encoding='utf-8-sig',
        index=False)
    pd.DataFrame(search_again).to_csv(
        os.path.join("f_web_data", "google_trends", f'try_again.csv'),
        encoding="utf-8",
    )
    pd.DataFrame(err_400).to_csv(
        os.path.join("f_web_data", "google_trends", f'err_400.csv'),
        encoding="utf-8",
    )




def run_update(engine):
    """Continues to update tables:
            L1WEBDATA.GOOGLE_TRENDS.*
    excluding:
            L1WEBDATA.GOOGLE_TRENDS._*

    Arguments:
        engine (sqlAlchemy.engine): for the database connection.
    """
    get_regional_data(engine)
 

def run(mode: str):
    """Main entrance of loading script.

    Arguments:
        mode (str): in which mode to manipulate the database.
    """
    eng = sqlalchemy_engine_sf(
        'L1WEBDATA', 'GOOGLE_TRENDS', role='DBADMIN_L1WEBDATA')
    eng.execute('UPDATE "_LOOKUP_SET_KEYWORD" '
                '   SET "LOCKED" = 0 '
                ' WHERE TRUE;')
    MODE_FUNCTION_MAP[mode](eng)
    eng.dispose()


# Mapping the mode parameter to the function that should run
MODE_FUNCTION_MAP = {
   
    'update': run_update,
    
}
