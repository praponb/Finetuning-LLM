from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("country_length", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("country").orderBy(F.col("launch_date").desc(), "id")

valid = (
    df_suppliers
    .filter(F.col("city") != "New York")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.length("country").alias("country_length"),
        F.length("city").alias("city_length"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

latest = (
    with_latest
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("YEAR", "launch_date").alias("year_start"),
        F.when(F.col("city_length") >= 70, "PREMIUM").otherwise("STANDARD").alias("city_length_tier"),
    )
    .agg(
        F.percentile_approx("city_length", 0.5).alias("city_length_p50"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.percentile_approx("city_length", 0.5).alias("city_length_p50_2"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("city_length_p50_2").desc_nulls_last())
)
