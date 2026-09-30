from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-06-25").cast("date"))
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM-dd"))
    .withColumn("status_clean", F.coalesce("status", F.lit("NONE")))
)

cleaned_accounts = (
    cleaned
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-09-30"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "status_clean",
        F.when(F.col("balance") >= 76000, "High").when(F.col("balance") >= 19000, "Medium").otherwise("Low").alias("balance_level"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean_2"),
    )
)

metrics = (
    cleaned_accounts
    .filter(~F.col("account_type").isin(["CREDIT", "SAVINGS", "CHECKING"]))
    .withColumn("running_min_balance", F.min("balance").over(w))
    .withColumn("running_avg_balance", F.avg("balance").over(w))
    .withColumn("running_max_balance", F.max("balance").over(w))
)

with_dims = (
    metrics
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select("s.*", F.col("d.credit_limit"), F.col("d.signup_date"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.opened_date",
        "src.status",
        "src.status_clean",
        "src.balance_level",
        "src.status_clean_2",
        "src.running_max_balance",
        "src.credit_limit",
        F.col("p.age"),
        F.col("p.city"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("age") >= 69, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
        "status",
    )
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_rows"),
        F.percentile_approx("running_max_balance", 0.95).alias("running_max_balance_p95"),
    )
)
