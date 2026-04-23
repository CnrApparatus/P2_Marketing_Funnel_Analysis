"""
generate_marketing_dataset.py

Creates a realistic Pan-India marketing dataset for a D2C brand (audio + wearable tech).
Outputs 5 tables with specified row counts and injects the requested anomalies.

Usage:
  - install: pip install pandas numpy faker sqlalchemy psycopg2-binary tqdm
  - update DB connection params in the CONFIG section if you want to push to Postgres
  - run: python generate_marketing_dataset.py

Files produced:
  - ./output/ad_campaigns.csv
  - ./output/ad_performance.csv
  - ./output/website_sessions.csv
  - ./output/leads.csv
  - ./output/conversions.csv
  - ./output/validation_report.json

NOTE: This script is intentionally verbose and commented for learning and reproducibility.
"""

import os
import random
import uuid
from datetime import datetime, timedelta
import json

import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm
from sqlalchemy import create_engine, text

# -------------------------------
# CONFIG
# -------------------------------
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker('en_IN')
Faker.seed(SEED)

OUTPUT_DIR = 'output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Target rows (as requested)
N_CAMPAIGNS = 120
N_AD_PERF = 300_000
N_SESSIONS = 400_000
N_LEADS = 70_000
N_CONVERSIONS = 15_000

# Date range for synthetic activity (inclusive)
DATE_START = datetime(2024, 1, 1)
DATE_END = datetime(2026, 3, 1)
DAYS_TOTAL = (DATE_END - DATE_START).days + 1

# Postgres config: change if you want to push directly to Postgres
PG_CONFIG = {
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432,
    'db': 'marketing_analytics_db'
}

# Percentage knobs for anomalies
DUPLICATE_PCT = 0.025        # 2.5% duplicate rows
NULL_PCT = 0.04              # 4% nulls
INCONSISTENT_CAT_PCT = 0.03  # 3% inconsistent categorical values
CLICKS_GT_IMPR_PCT = 0.005   # 0.5% rows with clicks > impressions
COST_WITH_ZERO_CLICKS_PCT = 0.01  # 1% cost > 0 but clicks == 0
ORPHAN_CAMPAIGN_PCT = 0.01   # 1% rows referencing campaign IDs not in campaigns
CONV_WITHOUT_LEAD_PCT = 0.02 # 2% conversions whose user_id has no lead
LEAD_NO_SESSION_PCT = 0.02   # 2% leads with no session
CONV_DATE_BEFORE_SESSION_PCT = 0.01  # 1% conversions earlier than sessions
PERF_OUTSIDE_CAMPAIGN_PCT = 0.02     # 2% ad_performance outside campaign dates
OUTLIER_REVENUE_PCT = 0.002  # 0.2% extreme outlier revenues
MISSING_ATTR_PCT = 0.02      # 2% missing campaign attribution
DUP_LEADS_FOR_SAME_USER_PCT = 0.03  # 3% duplicate leads for same user
BOT_TRAFFIC_USER_PCT = 0.005  # 0.5% users act like bots
INCONSISTENT_USERID_PCT = 0.03  # 3% inconsistent user id formats

# -------------------------------
# HELPERS
# -------------------------------

def random_date(start, end):
    """Return a random datetime between `start` and `end`."""
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days),
                              seconds=random.randint(0, 86399))


def sample_campaign_dates():
    """Generate plausible start/end dates for campaigns."""
    s = random_date(DATE_START, DATE_END - timedelta(days=30))
    # duration: 7 to 180 days
    duration = random.randint(7, 180)
    e = s + timedelta(days=duration)
    # cap at DATE_END
    if e > DATE_END:
        e = DATE_END
    return s.date(), e.date()


def mk_campaign_id(n):
    return f'CAMP{n:04d}'


# -------------------------------
# 1) ad_campaigns
# -------------------------------

def create_ad_campaigns(n=N_CAMPAIGNS):
    channels = ['google', 'meta', 'email', 'affiliate', 'youtube', 'organic']
    campaigns = []
    for i in range(1, n + 1):
        cid = mk_campaign_id(i)
        channel = random.choices(channels, weights=[30, 30, 15, 10, 10, 5])[0]
        name = f"{channel.upper()}_{fake.word().capitalize()}_{i}"
        daily_budget = int(np.round(np.random.lognormal(mean=8, sigma=1.0)))  # skewed
        start_date, end_date = sample_campaign_dates()
        campaigns.append({
            'campaign_id': cid,
            'channel': channel,
            'campaign_name': name,
            'daily_budget': daily_budget,
            'start_date': pd.to_datetime(start_date),
            'end_date': pd.to_datetime(end_date)
        })
    df = pd.DataFrame(campaigns)

    # Introduce some inconsistent categorical values intentionally
    for _ in range(int(len(df) * 0.02)):
        idx = df.sample(1).index[0]
        df.loc[idx, 'channel'] = df.loc[idx, 'channel'].upper()  # e.g., GOOGLE

    df.to_csv(os.path.join(OUTPUT_DIR, 'ad_campaigns.csv'), index=False)
    return df


# -------------------------------
# 2) ad_performance
# -------------------------------

def create_ad_performance(ad_campaigns_df, n=N_AD_PERF):
    rows = []
    campaign_ids = ad_campaigns_df['campaign_id'].tolist()
    campaign_date_map = ad_campaigns_df.set_index('campaign_id')[['start_date', 'end_date']].to_dict('index')

    for _ in tqdm(range(n), desc='Generating ad_performance'):
        cid = random.choice(campaign_ids)
        # choose a date mostly within campaign dates but allow some outside
        s = campaign_date_map[cid]['start_date'].date()
        e = campaign_date_map[cid]['end_date'].date()
        # 95% chance within, else outside
        if random.random() < (1 - PERF_OUTSIDE_CAMPAIGN_PCT):
            d = random_date(pd.to_datetime(s), pd.to_datetime(e)).date()
        else:
            # outside: choose a date before start or after end
            if random.random() < 0.5:
                d = random_date(DATE_START, pd.to_datetime(s) - timedelta(days=1)).date()
            else:
                d = random_date(pd.to_datetime(e) + timedelta(days=1), DATE_END).date()

        impressions = int(abs(np.random.poisson(lam=2000)))
        # clicks as a binomial of impressions with small CTR
        clicks = int(np.random.binomial(impressions, p=min(0.15, np.random.beta(1.2, 40))))
        # cost correlated with clicks and some randomness
        cost = round(clicks * np.random.uniform(5, 60), 2)

        rows.append({
            'date': pd.to_datetime(d),
            'campaign_id': cid,
            'impressions': impressions,
            'clicks': clicks,
            'cost': cost
        })

    df = pd.DataFrame(rows)

    # Inject anomalies
    # 1) clicks greater than impressions for a small fraction
    idxs = df.sample(frac=CLICKS_GT_IMPR_PCT).index
    for i in idxs:
        df.at[i, 'clicks'] = int(df.at[i, 'impressions'] + random.randint(1, 50))

    # 2) cost with zero clicks
    idxs = df[df['clicks'] == 0].sample(frac=COST_WITH_ZERO_CLICKS_PCT, replace=False).index
    df.loc[idxs, 'cost'] = df.loc[idxs, 'cost'].apply(lambda x: round(max(1.0, x + np.random.exponential(20)), 2))

    # 3) Orphan campaign ids
    orphan_count = int(len(df) * ORPHAN_CAMPAIGN_PCT)
    for i in df.sample(orphan_count).index:
        df.at[i, 'campaign_id'] = f'ORPHAN_{random.randint(1000,9999)}'

    # 4) Inconsistent categorical values in campaign IDs or channels are not typical here; we already altered campaigns

    # 5) duplicates
    dup_count = int(len(df) * DUPLICATE_PCT)
    if dup_count > 0:
        duplicates = df.sample(dup_count)
        df = pd.concat([df, duplicates], ignore_index=True)
        df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)

    df.to_csv(os.path.join(OUTPUT_DIR, 'ad_performance.csv'), index=False)
    return df


# -------------------------------
# 3) website_sessions
# -------------------------------

def gen_user_id(idx):
    return f'user_{idx:07d}'


def create_website_sessions(n=N_SESSIONS, n_unique_users=100_000):
    # Create a pool of users (mix of formats)
    users = []
    for i in range(1, n_unique_users + 1):
        base = gen_user_id(i)
        # randomly create different formats to simulate inconsistency
        if random.random() < 0.03:
            users.append(str(uuid.uuid4()))
        elif random.random() < 0.05:
            users.append(str(i))
        else:
            users.append(base)

    devices = ['mobile', 'desktop', 'tablet']
    countries = ['India', 'IN', 'india', 'IND']

    rows = []
    for i in tqdm(range(n), desc='Generating website_sessions'):
        session_id = str(uuid.uuid4())
        user_id = random.choice(users)
        # majority of sessions should have campaign attribution, but we'll add missing later
        campaign_id = random.choice(list(pd.read_csv(os.path.join(OUTPUT_DIR, 'ad_campaigns.csv'))['campaign_id']) + [None]*1)
        session_date = random_date(DATE_START, DATE_END)
        device = random.choices(devices, weights=[0.75, 0.2, 0.05])[0]
        country = random.choice(countries)
        rows.append({
            'session_id': session_id,
            'user_id': user_id,
            'campaign_id': campaign_id,
            'session_date': pd.to_datetime(session_date),
            'device': device,
            'country': country
        })

    df = pd.DataFrame(rows)

    # Introduce bot traffic: pick small set of users who have many sessions in short time
    bot_users = random.sample(users, int(len(users) * BOT_TRAFFIC_USER_PCT))
    bot_rows = []
    for u in bot_users:
        burst_count = random.randint(200, 1200)
        burst_start = random_date(DATE_START, DATE_END - timedelta(days=1))
        for j in range(burst_count):
            bot_rows.append({
                'session_id': str(uuid.uuid4()),
                'user_id': u,
                'campaign_id': None,
                'session_date': pd.to_datetime(burst_start + timedelta(seconds=j)),
                'device': random.choice(['mobile', 'desktop']),
                'country': 'IN'
            })
    if bot_rows:
        df = pd.concat([df, pd.DataFrame(bot_rows)], ignore_index=True)

    # inconsistent device/country naming
    idxs = df.sample(frac=INCONSISTENT_CAT_PCT).index
    for i in idxs:
        df.at[i, 'device'] = df.at[i, 'device'].upper() if random.random() < 0.5 else df.at[i, 'device'].capitalize()
        df.at[i, 'country'] = df.at[i, 'country'].lower()

    # introduce missing campaign attribution
    df.loc[df.sample(frac=MISSING_ATTR_PCT).index, 'campaign_id'] = None

    # inconsistent user id formats (some numeric strings or UUIDs already present)
    # duplicates shouldn't be added here for sessions, but will be introduced in leads

    # duplicates rows small pct
    dup_count = int(len(df) * DUPLICATE_PCT)
    if dup_count > 0:
        df = pd.concat([df, df.sample(dup_count)], ignore_index=True).reset_index(drop=True)

    df.to_csv(os.path.join(OUTPUT_DIR, 'website_sessions.csv'), index=False)
    return df


# -------------------------------
# 4) leads
# -------------------------------

def create_leads(sessions_df, n=N_LEADS):
    # We'll sample user_ids from sessions but also introduce leads without sessions
    user_pool = sessions_df['user_id'].unique().tolist()
    rows = []

    for i in tqdm(range(1, n + 1), desc='Generating leads'):
        lid = f'LEAD{i:06d}'
        # choose user: mostly from existing sessions
        if random.random() < (1 - LEAD_NO_SESSION_PCT):
            user_id = random.choice(user_pool)
        else:
            # lead without session: create a new user id
            user_id = str(uuid.uuid4())

        lead_date = random_date(DATE_START, DATE_END)
        # attribute campaign sometimes missing
        if random.random() < 0.95:
            campaign_id = random.choice(list(pd.read_csv(os.path.join(OUTPUT_DIR, 'ad_campaigns.csv'))['campaign_id']))
        else:
            campaign_id = None

        rows.append({
            'lead_id': lid,
            'user_id': user_id,
            'lead_date': pd.to_datetime(lead_date),
            'campaign_id': campaign_id
        })

    df = pd.DataFrame(rows)

    # duplicate leads for same user
    dup_count = int(len(df) * DUP_LEADS_FOR_SAME_USER_PCT)
    if dup_count > 0:
        selected = df.sample(dup_count)
        df = pd.concat([df, selected], ignore_index=True)

    # introduce NULLs
    null_count = int(len(df) * NULL_PCT)
    if null_count > 0:
        for col in ['campaign_id']:
            df.loc[df.sample(null_count).index, col] = None

    # inconsistent user id formats for some leads
    if INCONSISTENT_USERID_PCT > 0:
        idxs = df.sample(frac=INCONSISTENT_USERID_PCT).index
        for i in idxs:
            if random.random() < 0.5:
                df.at[i, 'user_id'] = df.at[i, 'user_id'].replace('user_', '') if isinstance(df.at[i, 'user_id'], str) and df.at[i, 'user_id'].startswith('user_') else str(uuid.uuid4())

    df.to_csv(os.path.join(OUTPUT_DIR, 'leads.csv'), index=False)
    return df


# -------------------------------
# 5) conversions
# -------------------------------

def create_conversions(leads_df, sessions_df, n=N_CONVERSIONS):
    lead_user_pool = leads_df['user_id'].unique().tolist()
    session_user_pool = sessions_df['user_id'].unique().tolist()

    rows = []
    for i in tqdm(range(1, n + 1), desc='Generating conversions'):
        cid = f'CONV{i:06d}'
        # mostly convert users who were leads, but we'll introduce conversions without leads
        if random.random() < (1 - CONV_WITHOUT_LEAD_PCT):
            user_id = random.choice(lead_user_pool)
        else:
            user_id = random.choice(session_user_pool + [str(uuid.uuid4())])

        # conversion_date usually after lead_date and session_date, but we'll inject some anomalies
        conv_date = random_date(DATE_START, DATE_END)

        # revenue: typical revenue with some randomness, and outliers
        base_revenue = np.random.normal(loc=3000, scale=1200)
        revenue = max(50, round(base_revenue + np.random.normal(0, 300), 2))

        rows.append({
            'conversion_id': cid,
            'user_id': user_id,
            'revenue': revenue,
            'conversion_date': pd.to_datetime(conv_date),
            'campaign_id': random.choice(list(pd.read_csv(os.path.join(OUTPUT_DIR, 'ad_campaigns.csv'))['campaign_id']))
        })

    df = pd.DataFrame(rows)

    # conversion dates before session dates for some
    conv_count = int(len(df) * CONV_DATE_BEFORE_SESSION_PCT)
    if conv_count > 0:
        sample_conv = df.sample(conv_count)
        for idx in sample_conv.index:
            # pick a random session for this user if exists
            user = df.at[idx, 'user_id']
            user_sessions = sessions_df[sessions_df['user_id'] == user]
            if not user_sessions.empty:
                sess_date = user_sessions.sample(1)['session_date'].iloc[0]
                # force conversion to be earlier by a few days
                df.at[idx, 'conversion_date'] = pd.to_datetime(sess_date - timedelta(days=random.randint(1, 10)))

    # conversions without leads: change some user_ids to new random UUIDs already introduced above
    conv_no_lead_count = int(len(df) * CONV_WITHOUT_LEAD_PCT)
    if conv_no_lead_count > 0:
        idxs = df.sample(conv_no_lead_count).index
        for i in idxs:
            df.at[i, 'user_id'] = str(uuid.uuid4())

    # extreme outlier revenue values
    outlier_count = int(len(df) * OUTLIER_REVENUE_PCT)
    if outlier_count > 0:
        idxs = df.sample(outlier_count).index
        for i in idxs:
            df.at[i, 'revenue'] = round(df.at[i, 'revenue'] * random.randint(50, 500), 2)

    # introduce missing campaign attribution
    df.loc[df.sample(frac=MISSING_ATTR_PCT).index, 'campaign_id'] = None

    # duplicates
    dup_count = int(len(df) * DUPLICATE_PCT)
    if dup_count > 0:
        df = pd.concat([df, df.sample(dup_count)], ignore_index=True)

    df.to_csv(os.path.join(OUTPUT_DIR, 'conversions.csv'), index=False)
    return df


# -------------------------------
# NULL injection generic
# -------------------------------

def inject_nulls_generic(df, pct=NULL_PCT, cols=None):
    if cols is None:
        cols = df.columns.tolist()
    total = len(df) * len(cols)
    n_nulls = int(total * pct)
    for _ in range(n_nulls):
        ridx = random.randrange(0, len(df))
        c = random.choice(cols)
        df.at[ridx, c] = None
    return df


# -------------------------------
# WRITE TO POSTGRES (optional)
# -------------------------------

def push_to_postgres(dfs: dict, config=PG_CONFIG):
    # Create engine
    url = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['db']}"
    engine = create_engine(url)

    with engine.connect() as conn:
        conn.execute(text('commit'))
        # create schema / tables by replacing if exists
        for name, df in dfs.items():
            print(f'Pushing {name} ({len(df)} rows) to Postgres...')
            df.to_sql(name, engine, if_exists='replace', index=False, method='multi')
    print('Done pushing to Postgres (if reachable).')


# -------------------------------
# VALIDATION REPORT
# -------------------------------

def make_validation_report(dfs):
    report = {}
    for k, v in dfs.items():
        report[k] = {
            'rows': int(len(v)),
            'columns': list(v.columns),
            'null_counts': v.isna().sum().to_dict()
        }
    # additional checks
    # fraction of clicks > impressions
    if 'ad_performance' in dfs:
        ap = dfs['ad_performance']
        report['ad_performance']['clicks_gt_impressions_pct'] = float((ap['clicks'] > ap['impressions']).mean())
    if 'conversions' in dfs and 'leads' in dfs:
        conv = dfs['conversions']
        leads = dfs['leads']
        lead_users = set(leads['user_id'].dropna().unique())
        conv_users = set(conv['user_id'].dropna().unique())
        report['conversions']['users_without_leads_pct'] = float(len([u for u in conv_users if u not in lead_users]) / max(1, len(conv_users)))

    with open(os.path.join(OUTPUT_DIR, 'validation_report.json'), 'w') as f:
        json.dump(report, f, default=str, indent=2)
    return report


# -------------------------------
# MAIN
# -------------------------------

def main(push_db=False):
    print('1) Creating ad_campaigns...')
    ad_campaigns = create_ad_campaigns()

    print('2) Creating ad_performance...')
    ad_performance = create_ad_performance(ad_campaigns, n=N_AD_PERF)

    print('3) Creating website_sessions...')
    sessions = create_website_sessions(n=N_SESSIONS)

    print('4) Creating leads...')
    leads = create_leads(sessions, n=N_LEADS)

    print('5) Creating conversions...')
    conversions = create_conversions(leads, sessions, n=N_CONVERSIONS)

    # Inject general nulls into random tables
    ad_performance = inject_nulls_generic(ad_performance, pct=NULL_PCT, cols=['impressions', 'clicks', 'cost'])
    sessions = inject_nulls_generic(sessions, pct=NULL_PCT, cols=['device', 'country'])
    leads = inject_nulls_generic(leads, pct=NULL_PCT, cols=['campaign_id'])
    conversions = inject_nulls_generic(conversions, pct=NULL_PCT, cols=['revenue'])

    # Final saving (already partially saved), overwrite to ensure changes persisted
    ad_campaigns.to_csv(os.path.join(OUTPUT_DIR, 'ad_campaigns.csv'), index=False)
    ad_performance.to_csv(os.path.join(OUTPUT_DIR, 'ad_performance.csv'), index=False)
    sessions.to_csv(os.path.join(OUTPUT_DIR, 'website_sessions.csv'), index=False)
    leads.to_csv(os.path.join(OUTPUT_DIR, 'leads.csv'), index=False)
    conversions.to_csv(os.path.join(OUTPUT_DIR, 'conversions.csv'), index=False)

    dfs = {
        'ad_campaigns': ad_campaigns,
        'ad_performance': ad_performance,
        'website_sessions': sessions,
        'leads': leads,
        'conversions': conversions
    }

    print('Creating validation report...')
    report = make_validation_report(dfs)
    print(json.dumps(report, indent=2, default=str)[:1000])

    if push_db:
        push_to_postgres(dfs)

    print('All files saved under ./output')


if __name__ == '__main__':
    # by default, do NOT push to Postgres; set push_db=True to attempt insertion
    main(push_db=False)
