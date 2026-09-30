from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("days_since_shipped_date").asc_nulls_last())
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w3 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy("days_since_shipped_date", "id")
w5 = Window.partitionBy("carrier").orderBy(F.col("days_since_shipped_date").desc_nulls_first())
w6 = Window.partitionBy("carrier").orderBy(F.col("days_since_shipped_date_dense_rank").desc(), "id")

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-05-22").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("delivered_date_dayofweek", F.dayofweek("delivered_date"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("DIRECT")))
)

cleaned_shipments = (
    cleaned
    .filter(F.col("carrier").isNotNull() & (F.col("delivered_date") > F.lit("2023-03-11").cast("date")))
    .withColumn("days_since_shipped_date", F.datediff(F.current_date(), "shipped_date"))
    .withColumn("carrier_upper", F.upper("carrier"))
    .withColumn("carrier_filled_2", F.coalesce("carrier", F.lit("NONE")))
)

filtered = (
    cleaned_shipments
    .filter(F.col("shipped_date") > F.lit("2023-08-18").cast("date"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "delivered_date_dayofweek",
        "carrier_filled",
        "days_since_shipped_date",
        "carrier_upper",
        "carrier_filled_2",
        F.when(F.col("days_since_shipped_date") >= 73, "High").when(F.col("days_since_shipped_date") >= 30, "Medium").otherwise("Low").alias("days_since_shipped_date_band"),
    )
)

metrics = (
    filtered
    .withColumn("days_since_shipped_date_dense_rank", F.dense_rank().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("cumulative_delivered_date_dayofweek", F.sum("delivered_date_dayofweek").over(w3))
    .withColumn("days_since_shipped_date_bucket", F.ntile(10).over(w4))
    .withColumn("days_since_shipped_date_percent_rank", F.percent_rank().over(w5))
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .cube(
        F.when(F.col("days_since_shipped_date") >= 84, "LARGE").otherwise("SMALL").alias("days_since_shipped_date_tier"),
        F.date_trunc("month", "delivered_date").alias("activity_month"),
        "carrier",
    )
    .agg(
        F.round(F.sum("days_since_shipped_date_dense_rank") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("days_since_shipped_date_dense_rank_per_returned"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_rows"),
    )
    .filter(F.col("flash_rows") > 1)
    .orderBy(F.col("activity_month").desc())
)
