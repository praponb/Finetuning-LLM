from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("val_rounded").asc_nulls_last())
w3 = Window.orderBy(F.col("val").desc())
w4 = Window.orderBy(F.col("val_rounded").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("discounts").alias("val"))
)

base = (
    flattened
    .filter(F.col("effective_from") < "2024-12-01")
    .select(
        "*",
        F.date_format("effective_from", "yyyy-MM-dd").alias("effective_from_label"),
    )
)

base_price_tiers = (
    base
    .filter(F.col("effective_from") > F.lit("2024-03-06").cast("date"))
    .select(
        "*",
        F.coalesce("val", F.lit(0)).alias("val_filled"),
        F.round("val", 1).alias("val_rounded"),
    )
)

filtered = (
    base_price_tiers
    .filter(F.col("effective_from") > F.lit("2024-05-29").cast("date"))
    .select(
        "*",
        F.when(F.col("plan").isin(["pro", "enterprise"]), "Core").otherwise("Other").alias("plan_group"),
    )
)

enriched = (
    filtered
    .withColumn("plan_count_val_filled", F.count("val_filled").over(w_part))
    .withColumn("val_rounded_cume_dist", F.cume_dist().over(w2))
    .withColumn("val_dense_rank", F.dense_rank().over(w3))
    .withColumn("val_rounded_rank", F.rank().over(w4))
    .withColumn("plan_sum_val_filled", F.sum("val_filled").over(w_part))
)

result = (
    enriched
    .select(
        "id",
        "plan",
        "val",
        "effective_from_label",
        "val_filled",
        "plan_count_val_filled",
        "val_rounded_cume_dist",
        "val_dense_rank",
        "plan_sum_val_filled",
    )
    .orderBy(F.col("plan").desc(), F.col("plan_sum_val_filled").desc())
)
