from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w_part = Window.orderBy("adjusted_price", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("adjusted_price", "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("rating") >= 3.9, "High").when(F.col("rating") >= 1.9, "Medium").otherwise("Low").alias("rating_band"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

valid_suppliers = (
    valid
    .filter(F.col("city").isin(["Bangkok", "Berlin"]))
    .withColumn("clean_country", F.trim("country"))
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.8, "High").when(F.col("rating") >= 2.3, "Medium").otherwise("Low"))
)

latest_match = (
    valid_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.rating_band",
        "s.city_filled",
        "s.clean_country",
        "s.rating_tier",
        F.col("c.price"),
    )
)

cleaned = (
    latest_match
    .filter(F.col("country") == "TH")
    .withColumn("adjusted_price", F.round((F.col("price") * 0.85), 2))
)

filtered = (
    cleaned
    .filter(F.col("adjusted_price").between(56, 71))
    .select(
        "id",
        "country",
        "rating_band",
        "rating_tier",
        "price",
        "adjusted_price",
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.rating_band",
        "s.adjusted_price",
        "s.is_sg",
        F.col("c.launch_date"),
    )
)

metrics = (
    with_last
    .filter(F.col("adjusted_price") >= 60)
    .select(
        "id",
        "rating_band",
        "adjusted_price",
        "is_sg",
        "launch_date",
        F.max("adjusted_price").over(w_part).alias("last3_max_adjusted_price"),
        F.count("adjusted_price").over(w_part).alias("last3_count_adjusted_price"),
        F.sum("adjusted_price").over(w_part).alias("last3_sum_adjusted_price"),
        F.ntile(3).over(w3).alias("adjusted_price_bucket"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("last3_count_adjusted_price") >= 15, "LARGE").otherwise("SMALL").alias("last3_count_adjusted_price_tier"),
    )
    .agg(
        F.percentile_approx("last3_max_adjusted_price", 0.5).alias("last3_max_adjusted_price_p50"),
        F.percentile_approx("adjusted_price_bucket", 0.9).alias("adjusted_price_bucket_p90"),
        F.round(F.avg("adjusted_price"), 2).alias("avg_adjusted_price"),
    )
    .orderBy(F.col("last3_max_adjusted_price_p50").desc_nulls_first())
)
