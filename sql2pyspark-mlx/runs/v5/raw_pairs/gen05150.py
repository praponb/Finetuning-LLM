from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date_quarter").desc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("start_date_quarter").desc_nulls_first())
w3 = Window.partitionBy("plan").orderBy("start_date_quarter", "id")
w_part = Window.partitionBy("plan").orderBy("start_date_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("start_date_quarter_dense_rank", "id")
w6 = Window.partitionBy("customer_id", "plan")
w7 = Window.orderBy(F.col("cumulative_start_date_quarter").desc(), "id")

cleaned = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 200) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
        F.quarter("start_date").alias("start_date_quarter"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "plan", "end_date", "start_date_quarter")
)

metrics = (
    latest
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        F.dense_rank().over(w2).alias("start_date_quarter_dense_rank"),
        F.lag("start_date_quarter", 1).over(w3).alias("prev_start_date_quarter"),
        F.sum("start_date_quarter").over(w_part).alias("cumulative_start_date_quarter"),
    )
)

enriched = (
    metrics
    .withColumn("start_date_quarter_dense_rank_bucket", F.ntile(5).over(w5))
    .withColumn("start_date_quarter_dense_rank_share", F.round((F.col("start_date_quarter_dense_rank") / F.sum("start_date_quarter_dense_rank").over(w6)), 4))
    .withColumn("seq_no", F.row_number().over(w7))
)

result = (
    enriched
    .cube("seq_no")
    .agg(
        F.percentile_approx("start_date_quarter_dense_rank", [0.25, 0.5, 0.75]).alias("start_date_quarter_dense_rank_quartiles"),
        F.avg("start_date_quarter_dense_rank").alias("avg_start_date_quarter_dense_rank"),
        F.max(F.when(F.col("plan") == "standard", F.col("seq_no"))).alias("standard_max_seq_no"),
        F.round(F.avg("start_date_quarter_dense_rank_share"), 2).alias("avg_start_date_quarter_dense_rank_share"),
        F.grouping("seq_no").alias("gid"),
    )
    .orderBy(F.col("avg_start_date_quarter_dense_rank_share").desc_nulls_last())
)
