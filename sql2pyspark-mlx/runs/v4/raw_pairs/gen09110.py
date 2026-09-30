from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

with_latest = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.balance"),
        F.col("c.opened_date").alias("account_opened_date"),
    )
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("age") >= 57, "HIGH").otherwise("LOW").alias("age_tier"),
        "segment",
    )
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.percentile_approx("credit_limit", 0.5).alias("credit_limit_p50"),
    )
    .orderBy(F.col("consumer_count").desc())
)
