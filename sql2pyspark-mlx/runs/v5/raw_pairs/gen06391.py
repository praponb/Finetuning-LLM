from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("adjusted_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("running_count_unit_price").desc_nulls_last(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("running_avg_unit_price").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("event_seq").desc())
w6 = Window.partitionBy("product_id").orderBy("running_sum_unit_price")
w7 = Window.orderBy("running_sum_unit_price", "id")
w8 = Window.orderBy("running_sum_unit_price")
w9 = Window.orderBy("latest_rank", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("unit_price") <= 290)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("unit_price") * 0.9), 2).alias("adjusted_unit_price"),
        F.when(F.col("unit_price") >= 320, "High").when(F.col("unit_price") >= 80, "Medium").otherwise("Low").alias("unit_price_band"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "product_id",
        "unit_price",
        "adjusted_unit_price",
        F.count("unit_price").over(w).alias("running_count_unit_price"),
        F.avg("unit_price").over(w).alias("running_avg_unit_price"),
        F.sum("unit_price").over(w).alias("running_sum_unit_price"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    ranked
    .withColumn("latest_rank_2", F.rank().over(w5))
    .filter(F.col("latest_rank_2") == 1)
    .select("id", "product_id", "running_sum_unit_price", "latest_rank")
)

result = (
    latest
    .select(
        "id",
        "product_id",
        F.rank().over(w6).alias("running_sum_unit_price_rank"),
        F.lead("running_sum_unit_price", 1, 0).over(w7).alias("next_running_sum_unit_price"),
        F.cume_dist().over(w8).alias("running_sum_unit_price_cume_dist"),
        F.avg("running_sum_unit_price").over(w9).alias("running_sum_unit_price_avg_6"),
    )
)
