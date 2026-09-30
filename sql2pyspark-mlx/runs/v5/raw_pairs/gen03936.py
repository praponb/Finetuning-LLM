from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id").orderBy("quantity_filled", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("store_id").orderBy(F.col("seq_no").desc_nulls_last(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-07-15")))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

cleaned = (
    valid
    .filter(F.col("last_restock") >= F.lit("2023-04-05").cast("date"))
    .select(
        "*",
        F.when(F.col("quantity_filled") >= 71, "High").when(F.col("quantity_filled") >= 23, "Medium").otherwise("Low").alias("quantity_filled_level"),
        F.when(F.col("quantity") >= 267, "High").when(F.col("quantity") >= 119, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

metrics = (
    cleaned
    .filter(F.col("quantity") <= 198)
    .select(
        "id",
        "store_id",
        "quantity_band",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(10).over(w2).alias("quantity_bucket"),
        F.avg("quantity").over(w3).alias("store_id_avg_quantity"),
        F.avg("quantity_filled").over(w4).alias("quantity_filled_avg_3_prior"),
        F.row_number().over(w).alias("seq_no"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region").alias("region"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.quantity_bucket")).alias("min_quantity_bucket"),
    )
    .orderBy("region")
)
