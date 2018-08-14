import os
import pandas as pd


def _pg_query2df(query, username, password, host, port, dbname):
    from sqlalchemy import create_engine
    pg_endpoint = ('postgresql://' + username + ':' + password + '@'
                   + host + ':' + str(port) + '/' + dbname)
    engine = create_engine(pg_endpoint)
    data_frame = pd.read_sql_query(query, engine)
    return data_frame


def _cached_pg_query2df(query, username, password, host, port, dbname,
                        expire_after_min=60, cache_path='./__pycache__'):
    import hashlib, time
    querymd5 = hashlib.md5(query.encode('utf-8')).hexdigest()
    full_path = os.path.join(cache_path, querymd5 + '.bz2')
    if (os.path.exists(full_path) and
        time.time() - os.stat(full_path).st_mtime < 60 * expire_after_min):
        print('Reading from file %s...' % full_path)
        return pd.read_pickle(full_path)

    print('Reading from database...')
    try:
        df = _pg_query2df(query, username, password, host, port, dbname)
    except:
        raise
    if not os.path.isdir(cache_path):
        os.makedirs(cache_path)
    df.to_pickle(full_path)
    return df


def cached_redshift_query2df(query, expire_after_min=60,
                             cache_path='./__pycache__'):
    username = os.environ.get('REDSHIFT_USERNAME', '')
    password = os.environ.get('REDSHIFT_PASSWORD', '')
    host = os.environ.get('REDSHIFT_HOST', '')
    port = os.environ.get('REDSHIFT_PORT', 5439)
    dbname = os.environ.get('REDSHIFT_DATABASE', '')
    return _cached_pg_query2df(query, username, password, host, port,
                               dbname, expire_after_min, cache_path)
