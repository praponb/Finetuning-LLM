from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sqft").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("row_num", "id").rowsBetween(-6, -1)
w3 = Window.orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("store_id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2024-03-30").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.when(F.col("quantity") >= 257, "High").when(F.col("quantity") >= 103, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.sqft"))
)

latest = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

filtered = (
    latest
    .filter((F.col("last_restock") >= F.lit("2023-03-28").cast("date")) & (F.col("quantity") < 142))
    .withColumn("quantity_per_row_num", F.round((F.col("quantity") / F.col("row_num")), 2))
)

result = (
    filtered
    .withColumn("sqft_avg_6_prior", F.avg("sqft").over(w2))
    .withColumn("quantity_avg_6", F.avg("quantity").over(w3))
    .withColumn("store_id_stddev_samp_quantity", F.stddev_samp("quantity").over(w4))
)
