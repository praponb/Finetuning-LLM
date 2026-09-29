from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2024-03-18").cast("date"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.round((F.col("quantity") * 1.07), 2).alias("quantity_scaled"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("quantity") >= 234, "HIGH").otherwise("LOW").alias("quantity_tier"),
        F.date_trunc("YEAR", "last_restock").alias("year_start"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity_scaled", 0.9).alias("quantity_scaled_p90"),
    )
    .filter(F.col("distinct_ids") < 1)
)
