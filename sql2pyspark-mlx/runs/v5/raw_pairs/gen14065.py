from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w_part = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w_ordered = Window.partitionBy("product_id").orderBy("revenue", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())
w5 = Window.partitionBy("product_id").orderBy(F.col("revenue_rank").asc_nulls_first(), "id")
w6 = Window.orderBy(F.col("seq_no").desc(), "id")
w7 = Window.orderBy("product_id_avg_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("revenue_bucket").desc(), "id")

base = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 7, 10]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        F.col("units").cast("string").alias("units_str"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.avg("revenue").over(w).alias("product_id_avg_revenue"),
        F.ntile(10).over(w_part).alias("revenue_bucket"),
        F.ntile(4).over(w_ordered).alias("revenue_bucket_2"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.rank().over(w4).alias("revenue_rank"),
    )
)

base_store_sales = (
    enriched
    .filter(F.col("revenue_rank") == 5)
    .withColumn("revenue_rank_whole", F.ceil("revenue_rank"))
)

latest = (
    base_store_sales
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "product_id_avg_revenue",
        "revenue_bucket",
        "revenue_rank",
        "revenue_rank_whole",
        F.ntile(4).over(w6).alias("seq_no_bucket"),
        F.sum("seq_no").over(w7).alias("running_seq_no"),
        F.ntile(10).over(w_running).alias("revenue_bucket_bucket"),
    )
)
