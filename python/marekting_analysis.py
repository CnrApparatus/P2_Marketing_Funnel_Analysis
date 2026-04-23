from sqlalchemy import create_engine, text
import pandas as pd

# Use environment variables in real projects (not hardcoded credentials)
engine = create_engine(
    "postgresql+psycopg2://chetan:Ch3tan567@localhost:5432/boat_marketing_analysis_db",
    pool_pre_ping=True
)

query = text("SELECT * FROM v_marketing_master_table")

# Read data safely
with engine.connect() as conn:
    df = pd.read_sql(query, conn)

# Basic sanity checks
print(df.shape)
print(df.columns)

df.head()


# Top performing campaigns
top_campaigns = (
    df.dropna(subset=["roas"])
    .sort_values(by="roas", ascending=False)
    .head(10)
)

print(top_campaigns)


# Loss_making campaigns
loss_df = df[df["roi"].fillna(0) < 0]

loss_df = loss_df.sort_values(by="roi", ascending=True)

print(loss_df)


# Waste Detection (Robust quantile logic)
spend_q75 = df["spend"].quantile(0.75)
conversion_q25 = df["conversions"].quantile(0.25)

waste_df = df[
    (df["spend"] >= spend_q75) &
    (df["conversions"] <= conversion_q25)
]

print(waste_df)


# Channel-wise performance
channel_perf = (
    df.groupby("channel", as_index=False)
      .agg(
          total_spend=("spend", "sum"),
          total_revenue=("revenue", "sum"),
          avg_roi=("roi", "mean"),
          avg_roas=("roas", "mean")
      )
      .sort_values(by="avg_roi", ascending=False)
)

print(channel_perf)


# Funnel Drop-off Analysis
funnel_df = (
    df[[
        "campaign_id",
        "drop_click_to_session",
        "drop_session_to_lead",
        "drop_lead_to_conversion"
    ]]
    .fillna(0)
    .sort_values(by="drop_lead_to_conversion", ascending=False)
)

print(funnel_df)


# Outlier Detection
rev_q99 = df["revenue"].quantile(0.99)

outliers = df[df["revenue"] >= rev_q99]

print(outliers)


# Efficiency Score
df["roas_norm"] = (df["roas"] - df["roas"].min()) / (df["roas"].max() - df["roas"].min())
df["roi_norm"] = (df["roi"] - df["roi"].min()) / (df["roi"].max() -  df["roi"].min())
df["conv_rate_norm"] = (
    (df["lead_to_conversion"] - df["lead_to_conversion"].min()) /
    (df["lead_to_conversion"].max() - df["lead_to_conversion"].min())
)

df["efficiency_score"] = (
    df["roas_norm"] * 0.4 +
    df["roi_norm"] * 0.3 +
    df["conv_rate_norm"] * 0.3
)

top_efficiency = df.sort_values(by="efficiency_score", ascending=False).head(10)

print(top_efficiency[["campaign_id", "efficiency_score"]])
