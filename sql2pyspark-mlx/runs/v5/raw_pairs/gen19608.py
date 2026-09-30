from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-05-13"))) & (~F.col("brand").eqNullSafe(F.lit("Stark"))))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.length("brand").alias("brand_length"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

result = (
    cleaned
    .groupBy("supplier_id", F.date_trunc("WEEK", "launch_date").alias("week_start"))
    .agg(
        F.stddev_samp("price").alias("price_stddev"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_rows"),
        F.stddev_samp("price").alias("price_stddev_2"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("brand_length")).otherwise(0)).alias("globex_brand_length"),
        F.round(F.sum("brand_length") / F.nullif(F.count(F.when(F.col("brand") == "Initech", 1)), F.lit(0)), 2).alias("brand_length_per_initech"),
        F.stddev_samp("price").alias("price_stddev_3"),
    )
    .orderBy(F.col("week_start").desc_nulls_first())
)
