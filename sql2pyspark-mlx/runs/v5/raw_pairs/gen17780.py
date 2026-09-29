from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy(F.col("units_str").desc_nulls_last())
w3 = Window.orderBy(F.col("units_str").asc_nulls_first())

with_latest = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.city", "s.region", "s.opened_date", F.col("c.units"))
)

eligible = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.opened_date")), "left_anti")
)

remaining_keys = (
    eligible
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("store_name").isNotNull())
        .select("id")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("region").isNotNull())
    .select(
        "region",
        "opened_date",
        "units",
        F.col("units").cast("double").alias("units_str"),
    )
)

result = (
    valid
    .filter(F.col("opened_date") <= "2023-12-01")
    .select(
        "region",
        "opened_date",
        F.rank().over(w2).alias("units_str_rank"),
        F.cume_dist().over(w3).alias("units_str_cume_dist"),
    )
)
