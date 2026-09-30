from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("city").isNull())
    .withColumn("city_prefix", F.substring("city", 1, 3))
    .withColumn("rating_whole", F.floor("rating"))
)

eligible = (
    base
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date").between("2023-12-24", "2024-11-08")), "left_anti")
)

filtered = (
    eligible
    .filter(F.col("rating") < 2.4)
    .select(
        "*",
        F.length("city").alias("city_length"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"))
)

grouped = (
    latest_match
    .rollup(
        "city",
        F.when(F.col("city_length") >= 82, "PREMIUM").otherwise("STANDARD").alias("city_length_tier"),
    )
    .agg(
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("rating_per_jp"),
        F.round(F.avg("city_length"), 2).alias("avg_city_length"),
        F.percentile_approx("city_length", 0.9).alias("city_length_p90"),
    )
    .filter(F.col("avg_city_length") > 0)
)

result = grouped.select("city", "city_length_tier", "rating_per_jp", "city_length_p90")
