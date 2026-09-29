from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date_minus_52d", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("sale_date_minus_52d").desc(), "id")
w_part = Window.orderBy(F.col("sale_date").desc(), "id")
w_recent = Window.orderBy("sale_date", "id")
w5 = Window.partitionBy("store_id").orderBy("event_seq", "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("event_seq").desc(), "id")

base = (
    df_store_sales
    .filter(F.col("store_id") == 4)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        F.date_sub("sale_date", 52).alias("sale_date_minus_52d"),
    )
)

metrics = (
    base
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("seq_no", F.row_number().over(w_part))
    .withColumn("row_seq_2", F.row_number().over(w_recent))
)

with_windows = (
    metrics
    .withColumn("next_event_seq", F.lead("event_seq", 1).over(w5))
    .withColumn("event_seq_bucket", F.ntile(10).over(w6))
    .withColumn("event_seq_bucket_2", F.ntile(5).over(w6))
)

result = (
    with_windows
    .groupBy("store_id")
    .agg(
        F.percentile_approx("event_seq_bucket_2", 0.5).alias("event_seq_bucket_2_p50"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") < 1)
    .orderBy(F.col("store_id").desc())
)
