from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("supplier_name").like("%Imports%"))
    .select(
        "*",
        F.round((F.col("rating") * 1.15), 2).alias("rating_scaled"),
        F.concat_ws("/", "country", "supplier_name").alias("country_supplier_name_key"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

result = (
    base
    .groupBy("city")
    .pivot("country", ["US", "TH", "JP"])
    .agg(F.max("rating_scaled").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
