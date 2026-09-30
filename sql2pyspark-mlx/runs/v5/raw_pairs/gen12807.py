from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city", "country")

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isin([12, 18, 23])), "left_anti")
)

remaining_keys = (
    eligible
    .select("city")
    .subtract(
        df_suppliers
        .filter(F.upper("supplier_name").like("%IAM%"))
        .select("city")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.product_name").isNotNull()), "left_anti")
)

filtered = (
    kept
    .filter(F.col("city").eqNullSafe(F.lit("Tokyo")))
    .withColumn("supplier_name_lower", F.lower("supplier_name"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("NONE")))
)

metrics = (
    filtered
    .select(
        "id",
        "city",
        "rating",
        "resolved_city",
        F.count("rating").over(w).alias("country_count_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
    )
)

result = (
    metrics
    .groupBy(
        "city",
        F.when(F.col("country_count_rating") >= 18, "HIGH").otherwise("LOW").alias("country_count_rating_tier"),
    )
    .agg(
        F.sum("rating_share").alias("total_rating_share"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "New York", F.col("country_count_rating")).otherwise(0)).alias("new_york_country_count_rating"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("num_cities") < 1000)
)
