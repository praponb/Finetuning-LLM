from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity")
w_part = Window.partitionBy("store_id").orderBy("quantity", "id")
w_recent = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2024-03-01")))
    .select(
        "id",
        "store_id",
        "quantity",
        F.when(F.col("quantity") >= 203, "High").when(F.col("quantity") >= 84, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
        F.lead("quantity", 2).over(w_part).alias("next_quantity"),
        F.sum("quantity").over(w_recent).alias("cumulative_quantity"),
        F.last("quantity", ignorenulls=True).over(w_recent).alias("last_known_quantity"),
        F.percent_rank().over(w_hist).alias("quantity_percent_rank"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"))
)

daily = (
    with_dims
    .cube("city")
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.max("last_known_quantity").alias("max_last_known_quantity"),
    )
)

result = (
    daily
    .select("city", "chiang_mai_rows", "max_last_known_quantity")
    .orderBy(F.col("city").desc())
)
