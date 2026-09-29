from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())
w3 = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

latest = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

kept_keys = (
    latest
    .select("id")
    .intersectAll(
        df_accounts
        .filter(F.month("opened_date").isin([1, 2, 9]))
        .select("id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("balance_percent_rank", F.percent_rank().over(w2))
    .withColumn("balance_dense_rank", F.dense_rank().over(w3))
)

enriched = (
    scored
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.segment"))
)

result = (
    enriched
    .groupBy(
        "customer_id",
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.round(F.avg("balance_dense_rank"), 2).alias("avg_balance_dense_rank"),
    )
    .filter(F.col("distinct_account_types") >= 1)
)
