from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("days_since_last_restock").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w_recent = Window.partitionBy("region", "store_id").orderBy("store_opened_date", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("region").orderBy(F.col("last3_max_next_quantity").desc_nulls_last(), "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-09-14").cast("date")) & (F.col("quantity") >= 184))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.lead("quantity", 2).over(w).alias("next_quantity"),
        F.row_number().over(w2).alias("row_seq"),
        F.lead("days_since_last_restock", 1).over(w3).alias("next_days_since_last_restock"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.store_name"), F.col("s.opened_date"))
)

joined = (
    enriched
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.city"),
        F.col("p.store_name").alias("store_store_name"),
        F.col("p.opened_date").alias("store_opened_date"),
    )
)

joined_inventory = (
    joined
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.last_restock",
        "s.last_restock_label",
        "s.next_quantity",
        "s.store_name",
        "s.city",
        "s.store_opened_date",
        F.col("b.city").alias("store_city"),
        F.col("b.region"),
        F.col("b.sqft"),
    )
)

scored = (
    joined_inventory
    .select(
        "id",
        "last_restock",
        "next_quantity",
        "city",
        "store_city",
        "region",
        "sqft",
        F.max("next_quantity").over(w_recent).alias("last3_max_next_quantity"),
        F.avg("next_quantity").over(w_recent).alias("last3_avg_next_quantity"),
        F.sum("next_quantity").over(w_recent).alias("last3_sum_next_quantity"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .select(
        "id",
        "next_quantity",
        "store_city",
        "region",
        "sqft",
        "last3_max_next_quantity",
        "last3_avg_next_quantity",
        "last3_sum_next_quantity",
    )
    .orderBy(F.col("sqft").desc_nulls_last(), "last3_max_next_quantity")
)
