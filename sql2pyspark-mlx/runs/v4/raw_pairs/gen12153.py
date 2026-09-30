from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("status_length", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("status_length").desc(), "id")
w3 = Window.partitionBy("plan", "status").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("status").orderBy(F.col("start_date").desc_nulls_first(), "id")
w5 = Window.partitionBy("plan").orderBy(F.col("status_length_bucket").desc_nulls_first())
w6 = Window.partitionBy("status")
w7 = Window.orderBy(F.col("next_monthly_fee").asc_nulls_first())

cleaned = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-07-19").cast("date")) & F.col("customer_id").isNotNull() & F.col("start_date").between("2024-12-01", "2025-06-01"))
    .withColumn("status_length", F.length("status"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_6"),
        F.ntile(10).over(w2).alias("status_length_bucket"),
        F.lead("monthly_fee", 1, 0).over(w3).alias("next_monthly_fee"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

kept_keys = (
    ranked
    .select("id", "status")
    .subtract(
        df_subscriptions
        .filter(F.col("plan").isin(["basic", "premium"]))
        .select("id", "status")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("status_length_bucket_rank", F.rank().over(w5))
    .withColumn("status_sum_monthly_fee", F.sum("monthly_fee").over(w6))
    .withColumn("next_monthly_fee_rank", F.rank().over(w7))
)
