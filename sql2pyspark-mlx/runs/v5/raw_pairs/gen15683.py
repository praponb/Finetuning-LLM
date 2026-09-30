from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock").between("2024-03-01", "2024-05-21"))
    .select(
        "*",
        F.col("quantity").cast("string").alias("quantity_num"),
        F.round((F.col("quantity") * 1.15), 2).alias("quantity_adjusted"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("quantity_adjusted") >= 48)
    .withColumn("product_id_min_quantity", F.min("quantity").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_num",
        "quantity_adjusted",
        "product_id_min_quantity",
    )
)

enriched = (
    ranked
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.sqft"), F.col("s.city"), F.col("s.region"))
)

valid = (
    enriched
    .filter((F.col("last_restock") > F.lit("2023-08-17").cast("date")) & (F.col("city").eqNullSafe(F.lit("London"))))
    .withColumn("quantity_whole", F.floor("quantity"))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
)

result = (
    valid
    .withColumn("activity_month", F.date_trunc("MONTH", "last_restock"))
    .groupingSets(
        [["store_id", "product_id", "activity_month"], ["store_id", "product_id"], ["store_id"], []],
        "store_id",
        "product_id",
        "activity_month",
    )
    .agg(
        F.count_if(F.col("region") == "Central").alias("central_events"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.grouping("store_id").alias("gid"),
    )
    .orderBy("singapore_count")
)
