from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy("opened_date", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("region").orderBy("store_sale_units", "id")
w5 = Window.orderBy("store_sale_sale_date", "id")

scored = (
    df_stores
    .withColumn("city_min_sqft", F.min("sqft").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

with_latest = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.units").alias("store_sale_units"),
    )
)

metrics = (
    with_latest
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "city_min_sqft",
        "store_sale_units",
        F.lead("store_sale_units", 1).over(w4).alias("next_store_sale_units"),
        F.row_number().over(w5).alias("event_seq_2"),
    )
)

cleaned = (
    metrics
    .filter(~F.col("region").eqNullSafe(F.lit("North")))
    .select(
        "*",
        F.ceil("next_store_sale_units").alias("next_store_sale_units_whole"),
        F.round((F.col("city_min_sqft") * 1.07), 2).alias("adjusted_city_min_sqft"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.revenue").between(2100, 2500)), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "next_store_sale_units",
        "event_seq_2",
        "region_filled",
    )
    .orderBy(F.col("next_store_sale_units").desc_nulls_first())
)
