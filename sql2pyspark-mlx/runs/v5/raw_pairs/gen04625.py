from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_first(), "id")

eligible = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.merchant").isNotNull()), "left_semi")
)

cleaned = (
    eligible
    .filter((F.col("opened_date") >= F.lit("2023-06-08").cast("date")) & F.col("account_type").isin(["CHECKING", "CREDIT", "SAVINGS"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.when(F.col("status").isin(["OPEN", "FROZEN"]), "Core").otherwise("Other").alias("status_group"),
        F.coalesce("status", F.lit("OTHER")).alias("resolved_status"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

valid = (
    latest
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isNull())
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
)

result = (
    valid
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date_month_end"))
    .withColumn("balance_tier", F.when(F.col("balance") >= 68000, "PREMIUM").otherwise("STANDARD"))
    .cube("activity_month", "balance_tier")
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "CREDIT", 1)), F.lit(0)), 2).alias("balance_per_credit"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("balance_stddev") < 1000000)
    .orderBy("balance_stddev")
)
