from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("adjusted_quantity")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("adjusted_quantity").desc(), "id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-10-30").cast("date")) & F.col("store_id").isin([1, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.round((F.col("quantity") * 1.15), 2).alias("adjusted_quantity"),
    )
)

enriched = (
    filtered
    .withColumn("adjusted_quantity_dense_rank", F.dense_rank().over(w))
    .withColumn("adjusted_quantity_bucket", F.ntile(5).over(w2))
    .withColumn("store_id_sum_adjusted_quantity", F.sum("adjusted_quantity").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

joined = (
    enriched
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.sqft"), F.col("b.city"), F.col("b.region"))
)

joined_inventory = (
    joined
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.price"))
)

cleaned = (
    joined_inventory
    .filter(F.col("adjusted_quantity_dense_rank").between(3, 4))
    .withColumn("event_seq_str", F.col("event_seq").cast("double"))
    .withColumn("price_level", F.when(F.col("price") >= 980, "High").when(F.col("price") >= 330, "Medium").otherwise("Low"))
)

result = (
    cleaned
    .groupBy("city")
    .pivot("region", ["East", "North"])
    .agg(
        F.sum("store_id_sum_adjusted_quantity").alias("amount"),
        F.count(F.lit(1)).alias("cnt"),
    )
    .select(
        "city",
        F.col("East_amount").alias("east_amount"),
        F.col("East_cnt").alias("east_cnt"),
        F.col("North_amount").alias("north_amount"),
        F.col("North_cnt").alias("north_cnt"),
    )
)
