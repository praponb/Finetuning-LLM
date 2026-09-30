from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")
w3 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id", "store_id").orderBy("first_known_revenue", "id")
w5 = Window.partitionBy("store_id", "product_id").orderBy(F.col("row_num").desc(), "id")
w6 = Window.partitionBy("store_id").orderBy("row_num", "id")
w7 = Window.partitionBy("product_id").orderBy(F.col("prev_units").desc(), "id")
w8 = Window.partitionBy("store_id")
w9 = Window.partitionBy("product_id").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("prev_units").desc(), "id")
w11 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_last())

scored = (
    df_store_sales
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("prev_units", F.lag("units", 1).over(w2))
    .withColumn("first_known_revenue", F.first("revenue", ignorenulls=True).over(w3))
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

deduped_store_sales = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    deduped_store_sales
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

deduped_store_sales_2 = (
    latest
    .withColumn("row_num_2", F.row_number().over(w7))
    .filter(F.col("row_num_2") == 1)
    .drop("row_num_2")
)

enriched = (
    deduped_store_sales_2
    .withColumn("store_id_count_event_seq", F.count("event_seq").over(w8))
    .withColumn("running_revenue", F.sum("revenue").over(w9))
    .withColumn("prev_units_bucket", F.ntile(4).over(w10))
    .withColumn("revenue_percent_rank", F.percent_rank().over(w11))
)

result = (
    enriched
    .withColumn("week_start", F.date_trunc("WEEK", "sale_date"))
    .rollup("week_start", "product_id")
    .agg(
        F.max("event_seq").alias("max_event_seq"),
        F.round(F.avg("prev_units_bucket"), 2).alias("avg_prev_units_bucket"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy(F.col("max_event_seq").desc())
)
