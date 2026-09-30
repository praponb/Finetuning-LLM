from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("brand") == "Initech"))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.upper(F.trim("product_name")).alias("product_name_normalized"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.rating"), F.col("s.supplier_name"), F.col("s.city"))
)

cleaned = (
    joined
    .filter((F.col("launch_date") >= F.lit("2024-03-28").cast("date")) & F.col("city").isNotNull() & F.col("brand").isNotNull())
    .withColumn("price_tier", F.when(F.col("price") >= 1110, "High").when(F.col("price") >= 540, "Medium").otherwise("Low"))
    .withColumn("brand_upper", F.upper("brand"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("rating") >= 4.3, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
        "supplier_id",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("brand") == "Acme").alias("acme_events"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("rating_per_stark"),
    )
)
