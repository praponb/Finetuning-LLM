from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-06-18"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        F.floor("sqft").alias("sqft_whole"),
        F.upper("region").alias("region_upper"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

result = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.sqft",
        "s.region_upper",
        F.col("c.status").alias("order_status"),
    )
)
