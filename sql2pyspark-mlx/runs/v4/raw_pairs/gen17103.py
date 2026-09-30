from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_month").orderBy(F.col("row_count").desc())

matched_keys = (
    df_subscriptions
    .select("plan")
    .intersectAll(
        df_subscriptions
        .filter(F.col("monthly_fee") > 80)
        .select("plan")
    )
)

kept_rows = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.date_trunc("MONTH", "start_date").alias("activity_month"),
        F.when(F.col("monthly_fee") >= 261, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("row_count"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
    )
    .withColumn("row_count_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("activity_month").desc_nulls_first())
)
