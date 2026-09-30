from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("account_type").orderBy("opened_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("credit_limit").desc(), "id")
w3 = Window.partitionBy("status")
w_part = Window.partitionBy("customer_id", "status").orderBy("opened_date_plus_months", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-04-14")))
    .select(
        "*",
        F.add_months("opened_date", 3).alias("opened_date_plus_months"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.name").isNotNull())
    .select("src.*", F.col("d.name"), F.col("d.credit_limit"))
)

metrics = (
    joined
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "opened_date_plus_months",
        "status_filled",
        "credit_limit",
        F.sum("balance").over(w_recent).alias("cumulative_balance"),
        F.sum("credit_limit").over(w_recent).alias("cumulative_credit_limit"),
        F.ntile(3).over(w2).alias("credit_limit_bucket"),
        F.count("balance").over(w3).alias("status_count_balance"),
    )
)

enriched = (
    metrics
    .withColumn("last7_max_credit_limit_bucket", F.max("credit_limit_bucket").over(w_part))
    .withColumn("last7_avg_credit_limit_bucket", F.avg("credit_limit_bucket").over(w_part))
    .withColumn("last7_min_credit_limit_bucket", F.min("credit_limit_bucket").over(w_part))
)

result = (
    enriched
    .groupBy(F.date_trunc("QUARTER", "opened_date_plus_months").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_rows"),
        F.max("status_count_balance").alias("max_status_count_balance"),
        F.min("last7_max_credit_limit_bucket").alias("min_last7_max_credit_limit_bucket"),
        F.percentile_approx("cumulative_balance", 0.5).alias("cumulative_balance_p50"),
        F.percentile_approx("cumulative_balance", 0.95).alias("cumulative_balance_p95"),
        F.round(F.sum("status_count_balance") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("status_count_balance_per_open"),
    )
    .orderBy(F.col("max_status_count_balance").asc_nulls_last())
)
