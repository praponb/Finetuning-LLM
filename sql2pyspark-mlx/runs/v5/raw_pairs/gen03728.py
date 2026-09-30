from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("latest_rank", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy(F.col("sqft").desc_nulls_first())

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-08-20").cast("date")) & F.col("product_id").isin([6, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    latest
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.store_name"), F.col("d.sqft"))
)

scored = (
    joined
    .filter(F.col("latest_rank") < 1)
    .withColumn("last3_avg_quantity", F.avg("quantity").over(w2))
    .withColumn("last3_min_quantity", F.min("quantity").over(w2))
    .withColumn("last3_count_quantity", F.count("quantity").over(w2))
    .withColumn("last3_sum_quantity", F.sum("quantity").over(w2))
    .withColumn("sqft_rank", F.rank().over(w3))
)

result = (
    scored
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .cube(F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.last3_min_quantity")).alias("max_last3_min_quantity"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("max_last3_min_quantity").asc_nulls_first())
)
