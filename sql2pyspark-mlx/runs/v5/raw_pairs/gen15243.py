from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("delivered_date_month_end").asc_nulls_first(), "id")
w3 = Window.partitionBy("status").orderBy(F.col("dedup_rank").desc(), "id")
w4 = Window.partitionBy("status").orderBy("dedup_rank", "id")
w5 = Window.partitionBy("status").orderBy("delivered_date_month_end", "id")
w6 = Window.orderBy(F.col("dedup_rank").desc(), "id")

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & F.col("status").isin(["LOST", "IN_TRANSIT", "RETURNED"]))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "status", "delivered_date_month_end", "status_prefix")
)

latest = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("row_seq_2", F.row_number().over(w4))
    .withColumn("seq_no_2", F.row_number().over(w6))
)

result = (
    metrics
    .select(
        "id",
        "status",
        "delivered_date_month_end",
        "status_prefix",
        "event_seq",
        "row_seq",
        "row_seq_2",
        "seq_no_2",
        F.when(F.col("status").rlike("^(RETURNED|DELIVERED)$"), "GROUP_B").when(F.col("status").rlike("^(IN_TRANSIT|LOST)$"), "PRIORITY").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").desc_nulls_first())
)
