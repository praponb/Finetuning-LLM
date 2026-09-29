from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("days_since_last_restock").desc_nulls_last())

filtered = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-02-27")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.brand"), F.col("b.product_name"))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.opened_date") >= "2025-07-28")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.quantity",
        "s.days_since_last_restock",
        "s.latest_rank",
        F.col("b.city"),
        F.col("b.opened_date"),
    )
)

daily = (
    with_dims
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 234, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["city", "quarter_start", "quantity_tier"], ["city", "quarter_start"], []],
        "city",
        "quarter_start",
        "quantity_tier",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    daily
    .select("city", "quarter_start", "city_list", "gid")
    .orderBy(F.col("city").desc_nulls_last())
)
