from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft_whole").asc_nulls_last())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-04-26").cast("date")) & F.col("region").isNotNull())
    .withColumn("sqft_whole", F.floor("sqft"))
    .withColumn("region_filled", F.coalesce("region", F.lit("DIRECT")))
)

deduped = (
    valid
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_last = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"))
)

eligible = (
    with_last
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") > "2025-03-01"), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_whole",
        "row_num",
        "sale_date",
    )
)
