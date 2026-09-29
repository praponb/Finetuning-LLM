from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc_nulls_first())
w_grp = Window.orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("status").orderBy(F.col("monthly_fee").asc_nulls_last())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

ranked = (
    df_subscriptions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        F.ntile(4).over(w_grp).alias("monthly_fee_bucket"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4).alias("monthly_fee_share"),
        F.cume_dist().over(w4).alias("monthly_fee_cume_dist"),
    )
)

kept_keys = (
    with_windows
    .select("customer_id", "id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("status").isin(["ACTIVE", "CANCELLED", "PAUSED"]))
        .select("customer_id", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.city").isNull())
    .select(
        "s.customer_id",
        "s.start_date",
        "s.monthly_fee",
        "s.monthly_fee_share",
        "s.monthly_fee_cume_dist",
        F.col("p.city"),
        F.col("p.age"),
    )
)

result = (
    joined
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "monthly_fee",
        "monthly_fee_cume_dist",
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("avg_account_balance").desc())
)
