from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("product_id").orderBy(F.col("store_id_avg_units").asc_nulls_first(), "id")

metrics = (
    df_store_sales
    .select(
        "*",
        F.avg("units").over(w).alias("store_id_avg_units"),
        F.avg("units").over(w2).alias("units_avg_5_prior"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.city"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.store_name"), F.col("p2.sqft"), F.col("p2.opened_date"))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "units_avg_5_prior",
        "store_name",
        "sqft",
        "dedup_rank",
    )
    .orderBy(F.col("store_id").desc_nulls_last())
)
