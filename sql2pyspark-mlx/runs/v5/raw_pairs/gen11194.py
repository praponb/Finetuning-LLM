from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("running_sum_unit_price").asc_nulls_first(), "id")

deduped = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .filter(F.col("order_id") == 26)
    .withColumn("running_sum_unit_price", F.sum("unit_price").over(w2))
    .withColumn("running_max_unit_price", F.max("unit_price").over(w2))
    .withColumn("running_count_unit_price", F.count("unit_price").over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

deduped_order_items = (
    enriched
    .withColumn("dedup_rank_2", F.row_number().over(w3))
    .filter(F.col("dedup_rank_2") == 1)
)

latest = (
    deduped_order_items
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

report = (
    latest
    .select(
        "id",
        "order_id",
        "quantity",
        "dedup_rank",
        "running_max_unit_price",
        "running_count_unit_price",
        "row_seq",
        "dedup_rank_2",
        "rn",
    )
    .orderBy(F.col("rn").asc_nulls_last())
)

result = (
    report
    .select(
        "id",
        "order_id",
        "quantity",
        "dedup_rank",
        "running_max_unit_price",
        "running_count_unit_price",
        "row_seq",
        "rn",
    )
    .orderBy(F.col("id").desc())
)
