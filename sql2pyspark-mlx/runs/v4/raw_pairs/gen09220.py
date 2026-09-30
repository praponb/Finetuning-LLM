from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("line_total").desc(), "id")
w2 = Window.orderBy(F.col("least_rank_in_group_line_total").asc_nulls_first())
w_grp = Window.orderBy(F.col("unit_price").desc(), "id")
w4 = Window.orderBy("least_rank_in_group_line_total_whole", "id").rowsBetween(-5, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

base_order_items = (
    deduped
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "unit_price",
        F.least("rank_in_group", "line_total").alias("least_rank_in_group_line_total"),
    )
)

filtered = (
    base_order_items
    .filter(F.col("least_rank_in_group_line_total") > 78)
    .select(
        "*",
        F.ceil("least_rank_in_group_line_total").alias("least_rank_in_group_line_total_whole"),
    )
)

scored = (
    filtered
    .withColumn("least_rank_in_group_line_total_cume_dist", F.cume_dist().over(w2))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("least_rank_in_group_line_total_avg_5", F.avg("least_rank_in_group_line_total").over(w4))
)

result = (
    scored
    .select("id", "unit_price", "least_rank_in_group_line_total_cume_dist")
    .orderBy(F.col("least_rank_in_group_line_total_cume_dist").desc_nulls_last())
)
