from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter((~F.col("region").eqNullSafe(F.lit("Central"))) & (F.col("opened_date") > F.lit("2023-08-19").cast("date")))
    .withColumn("city_title", F.initcap(F.trim("city")))
    .withColumn("adjusted_sqft", F.round((F.col("sqft") * 1.1), 2))
    .withColumn("region_filled", F.coalesce("region", F.lit("N/A")))
)

base = (
    filtered
    .filter(F.col("opened_date") > F.lit("2023-05-26").cast("date"))
    .select(
        "*",
        F.substring("region", 1, 2).alias("region_prefix"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled_2"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") <= F.col("s.opened_date")), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id") == 17), "left_semi")
)

deduped = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "adjusted_sqft",
        "region_filled",
    )
)

latest_match = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

result = (
    latest_match
    .groupBy("region")
    .pivot("city", ["Berlin", "Tokyo"])
    .agg(F.avg("inventory_quantity").alias("value_sum"))
    .select("region", F.col("Berlin").alias("berlin"), F.col("Tokyo").alias("tokyo"))
)
