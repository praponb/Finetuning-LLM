from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy(F.col("quantity").asc_nulls_last(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-01").cast("date")) & (F.upper(F.trim("store_name")) == "DOWNTOWN EXPRESS"))
    .withColumn("store_name_filled", F.coalesce("store_name", F.lit("Unknown")))
    .withColumn("region_filled", F.coalesce("region", F.lit("N/A")))
)

with_last = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.region",
        "s.store_name_filled",
        "s.region_filled",
        F.col("c.last_restock"),
        F.col("c.quantity"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units").isNotNull()), "left_semi")
)

latest = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "city", "region", "store_name_filled", "last_restock", "quantity")
)

with_latest = (
    latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

result = (
    with_latest
    .groupBy("city")
    .pivot("region", ["West", "South"])
    .agg(F.sum("store_sale_revenue").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
