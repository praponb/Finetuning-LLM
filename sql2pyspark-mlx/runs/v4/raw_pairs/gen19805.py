from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w2 = Window.orderBy(F.col("val").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("val"))
)

enriched = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("val_rank"),
        F.dense_rank().over(w2).alias("val_dense_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("val_rank").isNotNull())
    .select(
        "plan",
        "effective_from",
        "val",
        "val_rank",
        "val_dense_rank",
        F.date_format("effective_from", "MMM yyyy").alias("effective_from_label"),
        F.substring("plan", 1, 4).alias("plan_prefix"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("MONTH", "effective_from").alias("activity_month"))
    .agg(
        F.avg(F.when(F.col("plan") == "enterprise", F.col("val_rank"))).alias("enterprise_avg_val_rank"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
    )
    .orderBy(F.col("activity_month").asc_nulls_first())
)
