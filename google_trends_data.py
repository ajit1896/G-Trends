"""



Dev Note:

    Anochor picking relies on the ratio between max values for of the paired
    keywords, as one of the min values will be 0.
    
    Designed creterion for anochor chaining is to have adjacent anochors max
    value ratio higher than 20%. But to leave buffer room for the 20% criterion,
    this implementation uses 25%..40% as determining condition.

    Similarly, for connecting keywords with their anchors, also use 1..4 as
    threashold instead of 1..5.

    For indexing the anchoers, default anchor should be indexed as 0, smaller
    anchors should be negative, bigger anchors should be positive.

    Need to record the last tried anchor - add two columns, lower bound anchor
    index) and upper bound anchor (index), always try between them using binary
    search (not implemented, not it's just picking an arbitrary one).

    For 15 tries, only use preferred anchor, to try to build up the chain, then
    the other 15-20 tries if still outlier, i.e. lower than 20% than the
    farthest preferred anchor while higher than 20% than the current extreme
    keyword.



"""

import asyncio
import logging
import random
import time
import traceback
from collections import namedtuple
from decimal import Decimal

import pandas as pd
# import requests
from pytrends.request import TrendReq
from requests.exceptions import ProxyError, SSLError, Timeout

from f_web_data.utils import sqlalchemy_engine_sf
from f_web_data.utils.ip_proxy import proxy_ipidea

logger = logging.getLogger(__name__)
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

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

GEO_COUNTRY_MAPPING = {
    'CA': 'Canada',
}

GPARAM = namedtuple('GPARAM', ['cate', 'geo', 'timeframe', 'gprop'])

GGL_TRENDS = []  # using list to avoid using the *global* keyword


def re_init_pytrends():
    """Re-initializes the pytrends objects to avoid being blocked by Google.

    """
    logger.info('Re-initializing GGL_TRENDS object...')
    try:
        del GGL_TRENDS[:]  # Empty the wrapper
        time.sleep(2)  # space out call to proxy generator

        # Gets proxy with usr pwd
        # GGL_TRENDS.append(TrendReq(
        #     hl='en-CA', tz=300, timeout=(20, 50),))

        # Gets proxy with usr pwd
        GGL_TRENDS.append(
            TrendReq(
                hl='en-CA',
                tz=300,
                timeout=(20, 50),
                proxies=[
                ]))

        # Gets proxy with API
        # GGL_TRENDS.append(
        #     TrendReq(
        #         hl='en-CA',
        #         tz=300,
        #         timeout=(20, 50),
        #         proxies=[f'http://{proxy_ipidea.peer_ip_from_api()}']))

        # No proxy
        # GGL_TRENDS.append(TrendReq(hl='en-CA', tz=300, timeout=(10, 25)))
    except (ProxyError, SSLError):
        logger.info('(ProxyError, SSLError) in re_init_pytrends()')
    except Exception:
        logger.warning('Other uncaught error in re_init_pytrends()')
        traceback.print_exc()


def shallow_reset_database(engine):
    """Resets raw table and metadata status columns.
    
    Run for a clean run from scratch. Use once every month for repulling
    monthly data.

    Existing anchor values and associated ratio values won't be cleared.

    Args:
        engine (sqlAlchemy.engine): for the database connection.
    """
    logger.debug('Resetting database tables.')
    with engine.begin() as cnx:
        cnx.execute('TRUNCATE TABLE "GOOGLE_TRENDS_RAW";')
        cnx.execute('''
            UPDATE "_LOOKUP_SET_KEYWORD" 
            SET "LOCKED" = 0, 
                "NO_DATA" = 0, 
                "INDIVIDUAL_DONE" = 0, 
                "PAIRED_DONE" = 0, 
                "TRY_TIMES" = 0, 
                "ANCHOR_TRY_TIMES" = 0,
                "LOWER_BOUND_ANCHOR_INDEX" = -999999999, 
                "UPPER_BOUND_ANCHOR_INDEX" = 999999999
            WHERE TRUE;
            ''')

        # update the ANCHOR and other values for default anchor
        cnx.execute('''
            UPDATE _LOOKUP_SET_KEYWORD
            SET ANCHOR                           = _LOOKUP_SET.DEFAULT_ANCHOR,
                KEYWORD_DEFAULT_ANCHOR_AVG_RATIO = 1,
                KEYWORD_DEFAULT_ANCHOR_MAX_RATIO = 1,
                PAIRED_DONE                      = 1,
                PREFERRED_ANCHOR_INDEX           = 0
            FROM _LOOKUP_SET
            WHERE _LOOKUP_SET.ID = _LOOKUP_SET_KEYWORD.SET_ID
            AND _LOOKUP_SET.DEFAULT_ANCHOR = _LOOKUP_SET_KEYWORD.KEYWORD;
        ''')


def preprocess_paired_df(df, keyword, anchor):
    """Pre-processes the dataframe of paired searched returned from the API.

    - Drops unnecessary columns
    - Renames used columns.

    Args:
        df (pd.DataFrame): Raw dataframe returned from pytrends.
        keyword (str): Current keyword.
        anchor (str): Current anchor.

    Returns:
        pd.DataFrame: Processed dataframe.
    """
    df = df[[keyword, anchor]]
    df['keyword'] = keyword
    df['anchor'] = anchor
    df = df.rename(columns={
        keyword: 'value',
        anchor: 'anchor_value',
    }).reset_index()
    return df


def keyword_anchor_flag_values(df, keyword, anchor):
    """Calcualtes checks criteria values.

    Args:
        df (pd.DataFrame): Cleaned dataframe of paired search data.
        keyword (str): Current keyword.
        anchor (str): Current anchor.

    Returns:
        tuple: A tuple of (max_keyword_value, avg_keyword_value, 
            max_anchor_value, avg_anchor_value, keyword_anchor_max_ratio,
            keyword_anchor_avg_ratio,)
    """
    max_keyword_value = df['value'].max()
    avg_keyword_value = df['value'].mean()
    max_anchor_value = df['anchor_value'].max()
    avg_anchor_value = df['anchor_value'].mean()

    logger.info('%s - max_keyword_value: %s, %s - max_anchor_value: %s',
                keyword, max_keyword_value, anchor, max_anchor_value)
    logger.info('%s - avg_keyword_value: %s, %s - avg_anchor_value: %s',
                keyword, avg_keyword_value, anchor, avg_anchor_value)

    keyword_anchor_max_ratio = (
        max_keyword_value / max(0.0001, max_anchor_value))
    keyword_anchor_avg_ratio = (
        avg_keyword_value / max(0.0001, avg_anchor_value))

    return (
        max_keyword_value,
        avg_keyword_value,
        max_anchor_value,
        avg_anchor_value,
        keyword_anchor_max_ratio,
        keyword_anchor_avg_ratio,
    )


def verify_preferred_anchor(keyword_obj, keyword_override=None):
    """Verifies if pass-in anchor still qualifies for anchor or not.

    Args:
        keyword_obj (namedtuple): Tuple containing the information necessary
            for the keyword searching.
        keyword_override (str, optional): Keyword to use instead of keyword in
            the `keyword_obj` if provided. Defaults to None.

    Returns:
        bool: True if the keyword qualifies.
    """
    __MAX_TRY_TIMES = 15

    keyword = keyword_override or keyword_obj['keyword']
    anchor = keyword_obj['anchor']
    query_keywords = list(set([keyword, anchor]))

    try_time = 0
    while True:
        try:
            df = asyncio.run(
                get_df_multi(
                    query_keywords,
                    cate=CATEGORY_MAP[keyword_obj['cate']],
                    geo=keyword_obj['geo'],
                    timeframe=keyword_obj['timeframe'],
                    gprop=keyword_obj['gprop']))
            break
        except Exception:
            if try_time > __MAX_TRY_TIMES:
                return False
            else:
                try_time += 1
                re_init_pytrends()

    df = preprocess_paired_df(df, keyword, anchor)
    try:
        (
            _,
            _,
            _,
            _,
            keyword_anchor_max_ratio,
            keyword_anchor_avg_ratio,
        ) = keyword_anchor_flag_values(df, keyword, anchor)

        return (((keyword_obj['preferred_anchor_index'] < 0) and
                 (0.25 <= keyword_anchor_max_ratio <= 0.4) and
                 (0.2 <= keyword_anchor_avg_ratio <= 0.5)) or
                ((keyword_obj['preferred_anchor_index'] > 0) and
                 (2.5 <= keyword_anchor_max_ratio <= 4) and
                 (2 <= keyword_anchor_avg_ratio <= 5)))
    except:
        return False


def maintain_existing_chain(engine):
    """Maintains the preferred anchors chain.

    Args:
        engine (sqlAlchemy.engine): for the database connection.

    Raises:
        ValueError: If no Google Trends data for default anchor of a set.
    """
    with engine.begin() as cnx:
        # get the existing chain from the database
        curr_anchors = cnx.execute('''
            SELECT "SET_ID",
                "KEYWORD",
                "ANCHOR",
                "PREFERRED_ANCHOR_INDEX",
                "CATE",
                "GEO",
                "TIMEFRAME",
                "GPROP",
                "DEFAULT_ANCHOR"
            FROM "_LOOKUP_SET_KEYWORD"
                    LEFT JOIN "_LOOKUP_SET"
                            ON "_LOOKUP_SET_KEYWORD"."SET_ID" = "_LOOKUP_SET"."ID"
            WHERE "PREFERRED_ANCHOR_INDEX" IS NOT NULL
                AND "_LOOKUP_SET"."ON_OFF"
            ORDER BY ABS("PREFERRED_ANCHOR_INDEX");     
            ''').fetchall()

    logger.info('Validating the default anchors.')
    # pull the data for the anchor 0's
    for keyword_obj in curr_anchors:
        if keyword_obj['preferred_anchor_index'] == 0:
            success = False
            while True:
                try:
                    success = asyncio.run(
                        fill_raw_abs_vals(keyword_obj, engine))
                    break
                except Exception:
                    re_init_pytrends()
            if not success:
                raise ValueError(
                    f'Default anchor for set: {keyword_obj["set_id"]} has no data'
                )

    # starting from the anochor with smallest absolute value, i.e. -1 and 1
    # pull in pair with their anchor and see whether still valid
    for keyword_obj in curr_anchors:
        if keyword_obj['preferred_anchor_index'] != 0:
            set_id = keyword_obj['set_id']
            keyword = keyword_obj['keyword']

            #   if yes, keep using and go to next one
            if verify_preferred_anchor(keyword_obj):
                continue
            else:
                anchor = keyword_obj['anchor']
                logger.info(
                    '%s not suitable for anchor %s anymore. preferred_anchor_index - %s',
                    keyword, anchor, keyword_obj['preferred_anchor_index'])
                #   if no, select the similar ones,
                similar_keywords = engine.execute(f'''
                    WITH REF AS (
                        SELECT "KEYWORD_DEFAULT_ANCHOR_MAX_RATIO" AS "MAX_",
                            "KEYWORD_DEFAULT_ANCHOR_AVG_RATIO" AS "AVG_"
                        FROM "_LOOKUP_SET_KEYWORD"
                        WHERE "KEYWORD" = '{__escape_single_quote(keyword)}'
                        AND "SET_ID" = {set_id}
                    )
                    SELECT KEYWORD,
                        ABS(LOG(2, "KEYWORD_DEFAULT_ANCHOR_MAX_RATIO" / (
                            SELECT "MAX_"
                            FROM REF
                        ))) AS "DISTANCE_M",
                        ABS(LOG(2, "KEYWORD_DEFAULT_ANCHOR_AVG_RATIO" / (
                            SELECT "AVG_"
                            FROM REF
                        ))) AS "DISTANCE_A"
                    FROM "_LOOKUP_SET_KEYWORD"
                    WHERE "SET_ID" = {set_id}
                    AND "KEYWORD_DEFAULT_ANCHOR_MAX_RATIO" / (
                        SELECT "MAX_"
                        FROM REF
                    ) BETWEEN 0.33 AND 3
                    ORDER BY "DISTANCE_M", "DISTANCE_A";
                    ''').fetchall()
                logger.info('%s new keywords to try with',
                            len(similar_keywords))
                for row in similar_keywords:
                    if row['keyword'] in (keyword, anchor):
                        continue
                    elif verify_preferred_anchor(keyword_obj, row['keyword']):
                        with engine.begin() as cnx:
                            # update the new anchor as the new preferred anchor
                            cnx.execute(f'''
                                UPDATE "_LOOKUP_SET_KEYWORD"
                                SET "PREFERRED_ANCHOR_INDEX" = {keyword_obj['preferred_anchor_index']}
                                WHERE "KEYWORD" = '{__escape_single_quote(row['keyword'])}'
                                AND "SET_ID" = {set_id};
                            ''')

                            # cancel the old anchor
                            cnx.execute(f'''
                                UPDATE "_LOOKUP_SET_KEYWORD"
                                SET "PREFERRED_ANCHOR_INDEX" = NULL
                                WHERE "KEYWORD" = '{__escape_single_quote(keyword)}'
                                AND "SET_ID" = {set_id};
                            ''')

                            # update other keywords that are using the old anchor
                            cnx.execute(f'''
                                UPDATE "_LOOKUP_SET_KEYWORD"
                                SET "ANCHOR" = '{__escape_single_quote(row['keyword'])}'
                                WHERE "ANCHOR" = '{__escape_single_quote(keyword)}'
                                AND "SET_ID" = {set_id};
                            ''')

                            # TODO: update variable `curr_anchors`
                        break
                else:
                    # nothing qualifies
                    # erase the whole branch
                    logger.info(
                        'No suitable anchor found in the similar set. Erasing the branch.'
                    )
                    default_anchor = keyword_obj['default_anchor']
                    for i, keyword_obj_2 in enumerate(curr_anchors):
                        if (keyword_obj['set_id'] == keyword_obj_2['set_id'] and
                                keyword_obj['preferred_anchor_index'] *
                                keyword_obj_2['preferred_anchor_index'] > 0 and
                                abs(keyword_obj_2['preferred_anchor_index']) >=
                                abs(keyword_obj['preferred_anchor_index'])):
                            with engine.begin() as cnx:
                                # cancel the current anchor
                                cnx.execute(f'''
                                    UPDATE "_LOOKUP_SET_KEYWORD"
                                    SET "PREFERRED_ANCHOR_INDEX" = NULL
                                    WHERE "KEYWORD" = '{__escape_single_quote(keyword_obj_2['keyword'])}'
                                    AND "SET_ID" = {set_id};
                                ''')

                                # update other keywords that are using ths anchor to use the default anchor
                                cnx.execute(f'''
                                    UPDATE "_LOOKUP_SET_KEYWORD"
                                    SET "ANCHOR" = '{__escape_single_quote(default_anchor)}'
                                    WHERE "ANCHOR" = '{__escape_single_quote(keyword_obj_2['keyword'])}'
                                    AND "SET_ID" = {set_id};
                                ''')

                            # remove from list so not check again
                            del curr_anchors[i]


def one_keyword_to_scrape(engine):
    """Gets one keyword to scrape and all necessary information from database.

    Sets the lock status of the keyword to true so that other threads won't
    redo the work.

    Args:
        engine (sqlAlchemy.engine): for the database connection.

    Returns:
        dict: The keyword to scrape, and its other attributes.
    """
    with engine.begin() as cnx:
        try:
            keyword_obj = dict(
                cnx.execute(
                    'SELECT "SET_ID", "KEYWORD", "INDIVIDUAL_DONE", "PAIRED_DONE", '
                    '       "TRY_TIMES", "ANCHOR", "LOWER_BOUND_ANCHOR_INDEX", '
                    '       "UPPER_BOUND_ANCHOR_INDEX", "CATE", "GEO", '
                    '       "TIMEFRAME", "GPROP", "DEFAULT_ANCHOR" '
                    '   FROM "_LOOKUP_SET_KEYWORD" '
                    '           LEFT JOIN "_LOOKUP_SET" '
                    '                   ON "_LOOKUP_SET_KEYWORD"."SET_ID" = "_LOOKUP_SET"."ID" '
                    '   WHERE "LOCKED" = 0 AND "NO_DATA" = 0 '
                    '       AND "INDIVIDUAL_DONE" * "PAIRED_DONE" = 0 '  # at least something to do
                    '       AND "TRY_TIMES" < 35 '
                    '       AND "_LOOKUP_SET"."ON_OFF" '
                    '   ORDER BY "TRY_TIMES" ASC, "ANCHOR_TRY_TIMES" ASC'
                    '   LIMIT 1;').fetchone())
            cnx.execute(  # lock keyword to avoid interthread racing
                'UPDATE "_LOOKUP_SET_KEYWORD" '
                '    SET "LOCKED" = 1 '
                f' WHERE "KEYWORD" = \'{__escape_single_quote(keyword_obj["keyword"])}\' '
                f' AND "SET_ID" = {keyword_obj["set_id"]};')
            return keyword_obj
        except TypeError:
            return None


def persist_df(df, keyword_obj, engine, anchor=None):
    """Persists dataframes from google trends into database.
    
    Category and geo information will be appended into the dataframes.

    Args:
        df (pandas.DataFrame): the dataframe for the keyword to save into
            database.
        keyword_obj (namedtuple): Tuple containing the information necessary
            for the keyword searching.
        engine (sqlAlchemy.engine): for the database connection.
        anchor (str, optional): Anchor kryword being used for paired search.
            Defaults to None.
    """
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df['google_trends_cate'] = keyword_obj['cate']
    df['geo'] = GEO_COUNTRY_MAPPING[keyword_obj['geo']]
    df['timeframe'] = keyword_obj['timeframe']
    df['gprop'] = keyword_obj['gprop']
    df['set_id'] = keyword_obj['set_id']
    if anchor is not None:
        df['anchor'] = anchor
    df = df.rename(columns=str.upper)
    df.to_sql('google_trends_raw', con=engine, if_exists='append', index=False)


def __escape_single_quote(s):
    """Adds backslash before the single quotes in the string so that they are
    escaped in further string construction.

    Args:
        s (str): Input string.

    Returns:
        str: -
    """
    return s.replace('\'', '\\\'')


async def get_df_multi(keywords,
                       cate,
                       timeframe='today 5-y',
                       geo='CA',
                       gprop=''):
    """Sends request to Google Trends server for interest over time data.

    Args:
        keywords (list[str]): Keyword(s) to search for.
        cate (int): Google's API's interal category code.
        timeframe (str, optional): One param for Google Trends API. Defaults
            to 'today 5-y'.
        geo (str, optional): One param for Google Trends API. Defaults to 'CA'.
        gprop (str, optional): One param for Google Trends API. Defaults to ''.

    Returns:
        pd.DataFrame: Data returned from the API.
    """
    logger.info(
        'Pulling data for %s with cate - %s, timeframe - %s, gprop - %s ',
        str(keywords), cate, timeframe, gprop)
    GGL_TRENDS[0].build_payload(
        keywords, cat=cate, timeframe=timeframe, geo=geo, gprop=gprop)
    result = GGL_TRENDS[0].interest_over_time()
    await asyncio.sleep(random.randint(2, 8))
    return result


async def get_df_single(keyword,
                        cate,
                        timeframe='today 5-y',
                        geo='CA',
                        gprop=''):
    """Sends request to Google Trends server for interest over time data for a
    single keyword.

    Args:
        keyword (str): Keyword to search for.
        cate (int): Google's API's interal category code.
        timeframe (str, optional): One param for Google Trends API. Defaults
            to 'today 5-y'.
        geo (str, optional): One param for Google Trends API. Defaults to 'CA'.
        gprop (str, optional): One param for Google Trends API. Defaults to ''.

    Returns:
        pd.DataFrame: Data returned from the API.
    """
    return await get_df_multi(
        keywords=[keyword],
        cate=cate,
        timeframe=timeframe,
        geo=geo,
        gprop=gprop)


async def fill_raw_abs_vals(keyword_obj, engine):
    """Fills the raw table with the individual-search values for the keyword.

    Args:
        keyword_obj (namedtuple): Tuple containing the information necessary
            for the keyword searching.
        engine (sqlAlchemy.engine): for the database connection.

    Returns:
        bool: False if Google doesn't have data, else True
    """
    received_data = False
    keyword = keyword_obj['keyword']
    set_id = keyword_obj['set_id']
    # Cleans up raw table in case there is garbage data
    engine.execute('DELETE FROM "GOOGLE_TRENDS_RAW" '
                   f' WHERE "KEYWORD" = \'{__escape_single_quote(keyword)}\' '
                   f' AND "SET_ID" = {set_id} AND "ANCHOR" IS NULL;')

    logger.info('Start fetching individual: set_id - %s, keyword - %s', set_id,
                keyword)
    try:
        df = await get_df_single(
            keyword,
            cate=CATEGORY_MAP[keyword_obj['cate']],
            geo=keyword_obj['geo'],
            timeframe=keyword_obj['timeframe'],
            gprop=keyword_obj['gprop'])
        df = df[[keyword]]  # keep only useful column
        df['keyword'] = keyword
        df = df.rename(columns={keyword: 'value'}).reset_index()
        received_data = True
    except KeyError:
        logger.error(f'Keyword: search for "{keyword}" returned no data.')
        engine.execute(
            'UPDATE "_LOOKUP_SET_KEYWORD" '
            f'  SET "NO_DATA" = 1 '
            f' WHERE "KEYWORD" =\'{__escape_single_quote(keyword)}\' '
            f' AND "SET_ID" = {set_id};')
        return False
    except Timeout:
        logger.error(f'Timeout Error Occurred when searching for "{keyword}"')
        return False

    if received_data:
        persist_df(df, keyword_obj, engine, None)
        logger.info('Finished individual: set_id - %s, keyword - %s', set_id,
                    keyword)
        engine.execute(
            'UPDATE "_LOOKUP_SET_KEYWORD" '
            f'  SET "INDIVIDUAL_DONE" = 1 '
            f' WHERE "KEYWORD" =\'{__escape_single_quote(keyword)}\' '
            f' AND "SET_ID" = {set_id};')

    return True


def _search_for_new_anchor(set_id, keyword, try_times, engine):
    """Searches for a valid anchor in the current data and update the metadata
    table.

    Args:
        set_id (int): -
        keyword (str): -
        try_times (int): -
        engine (sqlAlchemy.engine): for the database connection.
    """
    logger.info('Current keyword doesn\'t work, searching for new one.')

    bound_index = engine.execute(
        'SELECT "LOWER_BOUND_ANCHOR_INDEX", "UPPER_BOUND_ANCHOR_INDEX" '
        '       FROM "_LOOKUP_SET_KEYWORD" '
        f' WHERE "KEYWORD" =\'{__escape_single_quote(keyword)}\' '
        f' AND "SET_ID" = {set_id};').fetchone()
    lower_bound_anchor_index = bound_index['lower_bound_anchor_index']
    upper_bound_anchor_index = bound_index['upper_bound_anchor_index']
    med_anchor_index = (lower_bound_anchor_index + upper_bound_anchor_index) / 2

    new_anchor = engine.execute(
        'SELECT "KEYWORD" FROM "_LOOKUP_SET_KEYWORD" '
        f' WHERE "SET_ID"={set_id} AND '
        f' "PREFERRED_ANCHOR_INDEX" > {lower_bound_anchor_index} AND '
        f' "PREFERRED_ANCHOR_INDEX" < {upper_bound_anchor_index} '
        f' ORDER BY ABS("PREFERRED_ANCHOR_INDEX" - ({med_anchor_index})) ASC '
        ' LIMIT 1;').scalar()

    if new_anchor:
        logger.info('Found new preferred achor: ' + new_anchor)
    elif try_times > 20:  # no preferred anchor and 20 times +
        if lower_bound_anchor_index >= 0:
            new_anchor = engine.execute(
                'SELECT "KEYWORD" FROM "_LOOKUP_SET_KEYWORD" '
                f' WHERE "SET_ID"={set_id}'
                ' ORDER BY KEYWORD_DEFAULT_ANCHOR_MAX_RATIO DESC LIMIT 1;'
            ).scalar()
        if upper_bound_anchor_index <= 0:
            new_anchor = engine.execute(
                'SELECT "KEYWORD" FROM "_LOOKUP_SET_KEYWORD" '
                f' WHERE "SET_ID"={set_id}'
                ' ORDER BY KEYWORD_DEFAULT_ANCHOR_MAX_RATIO ASC LIMIT 1;'
            ).scalar()

        if new_anchor:
            logger.info('Found new non-preferred achor: ' + new_anchor)

    if not new_anchor:
        logger.info('Didn\'t find new achor')
        new_anchor = ''

    engine.execute((
        'UPDATE "_LOOKUP_SET_KEYWORD" '
        f' SET "ANCHOR" = \'{new_anchor}\', "ANCHOR_TRY_TIMES" = "ANCHOR_TRY_TIMES"+1 '
        f' WHERE "KEYWORD" =\'{__escape_single_quote(keyword)}\' '
        f' AND "SET_ID" = {set_id};'))


async def fill_raw_rel_vals(keyword_obj, engine):
    """Fills the raw table with the group-search values for the keyword.

    Default anchor won't enter this function by design, which is achieved 
    by setting paired_done to 1.

    Args:
        keyword_obj (namedtuple): Tuple containing the information necessary
            for the keyword searching.
        engine (sqlAlchemy.engine): for the database connection.
    """
    set_id = keyword_obj['set_id']
    keyword = keyword_obj['keyword']
    anchor = keyword_obj['anchor'] or keyword_obj['default_anchor']
    # clean up raw table in case there is garbage data
    engine.execute('DELETE FROM "GOOGLE_TRENDS_RAW" '
                   f' WHERE "KEYWORD" = \'{__escape_single_quote(keyword)}\''
                   f' AND "SET_ID" = {set_id} AND "ANCHOR" IS NOT NULL;')

    try:
        query_keywords = list(set([keyword, anchor]))
        # Create empty df to skip further processing if only updating anchor
        df = pd.DataFrame()
        if anchor:
            df = await get_df_multi(
                query_keywords,
                cate=CATEGORY_MAP[keyword_obj['cate']],
                geo=keyword_obj['geo'],
                timeframe=keyword_obj['timeframe'],
                gprop=keyword_obj['gprop'])
        else:
            _search_for_new_anchor(set_id, keyword, keyword_obj['try_times'],
                                   engine)

        if not df.empty:
            # -- start -- preprocess df
            df = preprocess_paired_df(df, keyword, anchor)
            # -- end -- preprocess df

            # -- start -- validate whether the number/anchor is good fit
            (
                max_keyword_value,
                avg_keyword_value,
                max_anchor_value,
                avg_anchor_value,
                keyword_anchor_max_ratio,
                keyword_anchor_avg_ratio,
            ) = keyword_anchor_flag_values(df, keyword, anchor)

            # -- start -- calculate and update the ratio curr_keyword/default_anchor
            anchor_default_anchor_max_ratio = engine.execute(
                'SELECT "KEYWORD_DEFAULT_ANCHOR_MAX_RATIO" '
                '    FROM "_LOOKUP_SET_KEYWORD" '
                f' WHERE "KEYWORD" =\'{__escape_single_quote(anchor)}\' '
                f' AND "SET_ID" = {set_id};').scalar()
            keyword_default_anchor_max_ratio = (
                Decimal(keyword_anchor_max_ratio) *
                (anchor_default_anchor_max_ratio or 1))

            anchor_default_anchor_avg_ratio = engine.execute(
                'SELECT "KEYWORD_DEFAULT_ANCHOR_AVG_RATIO" '
                '    FROM "_LOOKUP_SET_KEYWORD" '
                f' WHERE "KEYWORD" =\'{__escape_single_quote(anchor)}\' '
                f' AND "SET_ID" = {set_id};').scalar()
            keyword_default_anchor_avg_ratio = (
                Decimal(keyword_anchor_avg_ratio) *
                (anchor_default_anchor_avg_ratio or 1))
            # -- end -- calculate and update the ratio curr_keyword/default_anchor

            criteria_pass = False
            if max_keyword_value > 20 and max_anchor_value > 20:
                criteria_pass = True
                logger.info('Anchor (%s) is suitable for keyword (%s)', anchor,
                            keyword)
            elif max_keyword_value <= 20:
                logger.info(('max_keyword_value <= 20, keyword (%s) is too '
                             'small for anchor (%s).'), keyword, anchor)
                engine.execute(
                    'UPDATE "_LOOKUP_SET_KEYWORD" '
                    f' SET "UPPER_BOUND_ANCHOR_INDEX" = IFNULL(( '
                    '    SELECT "PREFERRED_ANCHOR_INDEX" '
                    '       FROM  "_LOOKUP_SET_KEYWORD" '
                    f' WHERE "KEYWORD" =\'{__escape_single_quote(anchor)}\' '
                    f' AND "SET_ID" = {set_id}'
                    ' ), 999999999) '
                    f' WHERE "KEYWORD" =\'{__escape_single_quote(keyword)}\' '
                    f' AND "SET_ID" = {set_id};')
            elif max_anchor_value <= 20:
                logger.info(('max_anchor_value <= 20, keyword (%s) is too '
                             'big for anchor (%s).'), keyword, anchor)
                engine.execute(
                    'UPDATE "_LOOKUP_SET_KEYWORD" '
                    f' SET "LOWER_BOUND_ANCHOR_INDEX" = IFNULL(( '
                    '    SELECT "PREFERRED_ANCHOR_INDEX" '
                    '       FROM  "_LOOKUP_SET_KEYWORD" '
                    f' WHERE "KEYWORD" =\'{__escape_single_quote(anchor)}\' '
                    f' AND "SET_ID" = {set_id}'
                    ' ), -999999999) '
                    f' WHERE "KEYWORD" =\'{__escape_single_quote(keyword)}\' '
                    f' AND "SET_ID" = {set_id};')
            # -- end -- validate whether the number/anchor is good fit

            if criteria_pass:
                logger.info(
                    'Group search passed for: set_id - %s, keyword - %s',
                    set_id, keyword)
                persist_df(df, keyword_obj, engine, anchor)

                engine.execute(
                    'UPDATE "_LOOKUP_SET_KEYWORD" '
                    f' SET "PAIRED_DONE" = 1,"ANCHOR"=\'{__escape_single_quote(anchor)}\', '
                    f'  "MAX_KEYWORD_VALUE"={max_keyword_value},"MAX_ANCHOR_VALUE"={max_anchor_value},'
                    f'  "KEYWORD_ANCHOR_MAX_RATIO"={keyword_anchor_max_ratio},'
                    f'  "KEYWORD_DEFAULT_ANCHOR_MAX_RATIO"={keyword_default_anchor_max_ratio},'
                    f'  "AVG_KEYWORD_VALUE"={avg_keyword_value},"AVG_ANCHOR_VALUE"={avg_anchor_value},'
                    f'  "KEYWORD_ANCHOR_AVG_RATIO" = {keyword_anchor_avg_ratio}, '
                    f'  "KEYWORD_DEFAULT_ANCHOR_AVG_RATIO" = {keyword_default_anchor_avg_ratio} '
                    f' WHERE "KEYWORD" =\'{__escape_single_quote(keyword)}\' '
                    f' AND "SET_ID" = {set_id};')
                # update into preferred anchor chain
                if (((0.25 <= keyword_anchor_max_ratio <= 0.4) and
                     (0.2 <= keyword_anchor_avg_ratio <= 0.5)) or
                    ((2.5 <= keyword_anchor_max_ratio <= 4) and
                     (2 <= keyword_anchor_avg_ratio <= 5))):

                    anchor_preferred_anchor_index = engine.execute(
                        'SELECT "PREFERRED_ANCHOR_INDEX" '
                        '       FROM  "_LOOKUP_SET_KEYWORD" '
                        f' WHERE "KEYWORD" =\'{__escape_single_quote(anchor)}\' '
                        f' AND "SET_ID" = {set_id};').scalar()

                    if (0.25 <= keyword_anchor_max_ratio <=
                            0.4):  # smaller anchor

                        exist = engine.execute(
                            'SELECT "PREFERRED_ANCHOR_INDEX" '
                            '  FROM  "_LOOKUP_SET_KEYWORD" '
                            f' WHERE  "PREFERRED_ANCHOR_INDEX"  = ({anchor_preferred_anchor_index} - 1) '
                            f' AND "SET_ID" = {set_id};').scalar()
                        if not exist:
                            engine.execute(
                                'UPDATE "_LOOKUP_SET_KEYWORD" '
                                f'  SET "PREFERRED_ANCHOR_INDEX" = {anchor_preferred_anchor_index} - 1 '
                                f' WHERE "KEYWORD" =\'{__escape_single_quote(keyword)}\' '
                                f' AND "SET_ID" = {set_id};')
                            logger.info(
                                'New preferred anchor: keyword - %s, PREFERRED_ANCHOR_INDEX - %s',
                                keyword, str(anchor_preferred_anchor_index - 1))

                    else:  # bigger anchor

                        exist = engine.execute(
                            'SELECT "PREFERRED_ANCHOR_INDEX" '
                            '  FROM  "_LOOKUP_SET_KEYWORD" '
                            f' WHERE  "PREFERRED_ANCHOR_INDEX"  = ({anchor_preferred_anchor_index} + 1) '
                            f' AND "SET_ID" = {set_id};').scalar()
                        if not exist:
                            engine.execute(
                                'UPDATE "_LOOKUP_SET_KEYWORD" '
                                f'  SET "PREFERRED_ANCHOR_INDEX" = {anchor_preferred_anchor_index} + 1 '
                                f' WHERE "KEYWORD" =\'{__escape_single_quote(keyword)}\' '
                                f' AND "SET_ID" = {set_id};')
                            logger.info(
                                'New preferred anchor: keyword - %s, PREFERRED_ANCHOR_INDEX - %s',
                                keyword, str(anchor_preferred_anchor_index + 1))

            else:  # criteria_pass == False
                _search_for_new_anchor(set_id, keyword,
                                       keyword_obj['try_times'], engine)
        else:
            logger.info('Empty keyword: %s', keyword)
            engine.execute(
                'UPDATE "_LOOKUP_SET_KEYWORD" '
                f' SET "NO_DATA" = 1 '
                f' WHERE "KEYWORD" =\'{__escape_single_quote(keyword)}\' '
                f' AND "SET_ID" = {set_id};')

    except Timeout:
        logger.error(f'Timeout Error Occurred when searching for "{keyword}"')


async def fill_raw_table(keyword_obj, engine):
    """Scrapes the data for the passed in keyword, individually and in group
    with the keyword to try with.

    Args:
        keyword_obj (dict): The keyword to scrape, and its other attributes.
        engine (sqlAlchemy.engine): for the database connection.
    """
    valid_keyword = True
    if not keyword_obj['individual_done']:
        valid_keyword = await fill_raw_abs_vals(keyword_obj, engine)

    if valid_keyword and not keyword_obj['paired_done']:
        await fill_raw_rel_vals(keyword_obj, engine)


async def async_worker(engine):
    """Scrapes the keywords asynchronizely.

    Args:
        engine (sqlAlchemy.engine): for the database connection.
    """
    await asyncio.sleep(random.random() * 10)  # space out the threads
    re_init_pytrends()
    check_point = time.perf_counter()
    while True:
        if (time.perf_counter() - check_point) > 60:
            re_init_pytrends()
            check_point = time.perf_counter()

        keyword_obj = one_keyword_to_scrape(engine)  # Get and lock keyword
        if keyword_obj is not None:
            keyword = keyword_obj['keyword']
            set_id = keyword_obj['set_id']
            logger.info('Retrieved: set_id - %s, keyword - %s', set_id, keyword)

            try:
                container_len = len(GGL_TRENDS)
                await fill_raw_table(keyword_obj, engine)
            except Exception:
                traceback.print_exc()
                re_init_pytrends()
            finally:
                logger.info(  # TODO: change to logger.debug in the future
                    'Unlocking: set_id - %s, keyword - %s', set_id, keyword)
                engine.execute(
                    'UPDATE "_LOOKUP_SET_KEYWORD" '
                    f'SET "LOCKED" = 0, "TRY_TIMES" = "TRY_TIMES"+{container_len} '
                    f'WHERE "KEYWORD" =\'{__escape_single_quote(keyword)}\' '
                    f'AND "SET_ID" = {set_id};')
        else:
            break


async def main(engine):
    await asyncio.gather(
        async_worker(engine),
        async_worker(engine),
    )


def run_load(engine):
    """Cleans the database and updates tables:
            L1WEBDATA.GOOGLE_TRENDS.*
    excluding:
            L1WEBDATA.GOOGLE_TRENDS._*

    Arguments:
        engine (sqlAlchemy.engine): for the database connection.
    """
    shallow_reset_database(engine)
    maintain_existing_chain(engine)
    asyncio.run(main(engine))


def run_update(engine):
    """Continues to update tables:
            L1WEBDATA.GOOGLE_TRENDS.*
    excluding:
            L1WEBDATA.GOOGLE_TRENDS._*

    Arguments:
        engine (sqlAlchemy.engine): for the database connection.
    """
    asyncio.run(main(engine))


def run_reset_all(engine):
    """Resets the database tables:
            L1WEBDATA.GOOGLE_TRENDS.*
    excluding:
            L1WEBDATA.GOOGLE_TRENDS._*

    Arguments:
        engine (sqlAlchemy.engine): for the database connection.
    """
    with engine.begin() as cnx:
        cnx.execute('TRUNCATE TABLE "GOOGLE_TRENDS_RAW";')
        cnx.execute(
            'UPDATE "_LOOKUP_SET_KEYWORD" '
            'SET "LOCKED" = 0, "NO_DATA" = 0, "INDIVIDUAL_DONE" = 0, '
            '"PAIRED_DONE" = 0, "TRY_TIMES" = 0, "ANCHOR_TRY_TIMES" = 0, '
            '"ANCHOR" = NULL, "MAX_KEYWORD_VALUE" = NULL, "MAX_ANCHOR_VALUE" = NULL, '
            '"KEYWORD_ANCHOR_MAX_RATIO" = NULL, "KEYWORD_DEFAULT_ANCHOR_MAX_RATIO" = NULL, '
            '"AVG_KEYWORD_VALUE" = NULL, "AVG_ANCHOR_VALUE" = NULL, '
            '"KEYWORD_ANCHOR_AVG_RATIO" = NULL, "KEYWORD_DEFAULT_ANCHOR_AVG_RATIO" = NULL, '
            '"PREFERRED_ANCHOR_INDEX" = NULL, "LOWER_BOUND_ANCHOR_INDEX" = -999999999, '
            '"UPPER_BOUND_ANCHOR_INDEX" = 999999999 '
            'WHERE TRUE;')


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
    'load': run_load,
    'update': run_update,
    'reset_all': run_reset_all,
}
