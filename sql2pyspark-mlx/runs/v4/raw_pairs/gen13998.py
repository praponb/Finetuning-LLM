from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("end_date").desc(), "id")
w2 = Window.orderBy("end_date", "id")
w3 = Window.partitionBy("plan").orderBy(F.col("end_date").desc(), "id")
w4 = Window.partitionBy("plan").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy(F.col("row_seq").desc())
w6 = Window.partitionBy("customer_id", "status")
w7 = Window.partitionBy("plan", "customer_id").orderBy("seq_no", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy("seq_no", "id")

filtered = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2023-07-30").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.add_months("end_date", 3).alias("end_date_plus_months"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.first("seq_no", ignorenulls=True).over(w4).alias("first_known_seq_no"),
        F.dense_rank().over(w5).alias("row_seq_dense_rank"),
        F.max("seq_no").over(w6).alias("customer_id_max_seq_no"),
        F.avg("event_seq").over(w7).alias("event_seq_avg_2"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("basic_count") > 1)
)
