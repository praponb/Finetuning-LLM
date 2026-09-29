from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.array_contains("tags", "sale") & F.col("supplier_id").isin([27, 30]))
)

agg = (
    recent_products
    .groupBy(
        F.when(F.col("brand").isin(["Umbrella", "Stark"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)

result = agg.orderBy(F.col("stddev_price").desc())
