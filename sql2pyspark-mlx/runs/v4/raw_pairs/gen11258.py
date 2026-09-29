from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("product_id") == 3)
    .select("id", "quantity", F.round("quantity", 2).alias("quantity_rounded"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("quantity_rounded") >= 80, "PREMIUM").otherwise("STANDARD").alias("quantity_rounded_tier"),
    )
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.min("quantity").alias("min_quantity"),
        F.sum("quantity").alias("total_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.min("quantity_rounded").alias("min_quantity_rounded"),
        F.percentile_approx("quantity_rounded", [0.25, 0.5, 0.75]).alias("quantity_rounded_quartiles"),
    )
    .orderBy(F.col("min_quantity_rounded").desc())
)
