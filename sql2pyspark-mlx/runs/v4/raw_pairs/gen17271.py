from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").asc_nulls_last())
w2 = Window.partitionBy("element").orderBy(F.col("qty").desc_nulls_first())
w3 = Window.partitionBy("element")
w4 = Window.orderBy(F.col("element_sum_is_vip").desc_nulls_first())

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select("id", "coupon_codes", F.col("line.qty"), F.col("line.price"))
)

flattened = exploded.select("id", "qty", F.explode_outer("coupon_codes").alias("element"))

filtered = (
    flattened
    .filter(F.col("element").isNotNull() & (F.col("element") == "BUNDLE"))
    .withColumn("qty_band", F.when(F.col("qty") >= 4, "High").when(F.col("qty") >= 3, "Medium").otherwise("Low"))
)

base = (
    filtered
    .filter(F.col("element").isNotNull())
    .select(
        "*",
        F.col("qty").cast("string").alias("qty_str"),
        F.when(F.col("element") == "VIP", 1).otherwise(0).alias("is_vip"),
        F.coalesce("element", F.lit("N/A")).alias("element_clean"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("qty_percent_rank"),
        F.cume_dist().over(w2).alias("qty_cume_dist"),
        F.sum("is_vip").over(w3).alias("element_sum_is_vip"),
    )
)

metrics = (
    enriched
    .withColumn("element_max_is_vip", F.max("is_vip").over(w3))
    .withColumn("element_sum_is_vip_dense_rank", F.dense_rank().over(w4))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("qty_cume_dist") >= 0.68, "HIGH").otherwise("LOW").alias("qty_cume_dist_tier"),
        "element",
    )
    .agg(
        F.count(F.when(F.col("element") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.count("*").alias("num_rows"),
    )
)
