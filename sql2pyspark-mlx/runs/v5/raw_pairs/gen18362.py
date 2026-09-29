from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("store_id", "region").orderBy(F.col("quantity_filled").desc_nulls_first(), "id")

base = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-07-07").cast("date"))
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

joined = (
    base
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.quantity",
        "src.quantity_filled",
        F.col("s.opened_date"),
    )
)

joined_inventory = (
    joined
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.region"),
        F.col("p.sqft"),
        F.col("p.opened_date").alias("store_opened_date"),
    )
)

latest = (
    joined_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

base_inventory = (
    latest
    .filter(F.col("opened_date") <= "2023-03-13")
    .select(
        "id",
        "store_id",
        "quantity",
        "quantity_filled",
        "opened_date",
        "region",
        "sqft",
        "store_opened_date",
        (F.col("quantity") + F.col("sqft")).alias("quantity_plus_sqft"),
        F.greatest("sqft", "quantity").alias("greatest_sqft_quantity"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region"),
    )
)

latest_inventory = (
    base_inventory
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    latest_inventory
    .filter(F.col("opened_date") > "2024-11-01")
    .select(
        "id",
        "store_id",
        "quantity",
        "quantity_filled",
        "opened_date",
        "quantity_plus_sqft",
        "greatest_sqft_quantity",
        "resolved_region",
        "rank_in_group",
        F.ceil("quantity").alias("quantity_whole"),
        F.round((F.col("greatest_sqft_quantity") * 0.85), 2).alias("greatest_sqft_quantity_adjusted"),
    )
)

result = (
    valid
    .select(
        "id",
        "store_id",
        "quantity",
        "opened_date",
        "greatest_sqft_quantity",
        "rank_in_group",
        "greatest_sqft_quantity_adjusted",
    )
    .orderBy(F.col("id").desc())
)
