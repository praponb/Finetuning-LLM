from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("country").isin(["US", "SG"]))
    .select(
        "*",
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("rating") >= 4.1, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.sum(F.when(F.col("country") == "DE", F.col("rating")).otherwise(0)).alias("de_rating"),
    )
    .filter(F.col("num_countries") > 1)
)
