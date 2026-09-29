from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

ranked = (
    df_stores
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

kept = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.opened_date")), "left_semi")
)

cleaned = (
    kept
    .filter(F.col("region").isNotNull() & (F.col("sqft") >= 8878))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.concat_ws("/", "store_name", "city").alias("store_name_city_key"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.city", "s.region", "s.sqft", F.col("c.amount"))
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("amount") >= 1700, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        "region",
    )
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.min("sqft").alias("min_sqft"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("amount"))).alias("tokyo_total_amount"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_rows"),
    )
    .filter(F.col("min_sqft") > 0)
)
