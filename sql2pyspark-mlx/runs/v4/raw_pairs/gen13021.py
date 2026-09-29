from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "carrier").orderBy("delivered_date_month", "id")
w3 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(-5, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.month("delivered_date").alias("delivered_date_month"),
        F.coalesce("carrier", F.lit("Unknown")).alias("carrier_filled"),
    )
)

with_windows = (
    cleaned
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w))
    .withColumn("delivered_date_month_bucket", F.ntile(5).over(w2))
    .withColumn("delivered_date_month_avg_5", F.avg("delivered_date_month").over(w3))
)

cleaned_shipments = (
    with_windows
    .filter((F.col("delivered_date") > F.lit("2024-04-16").cast("date")) & F.col("carrier").isNotNull() & (F.col("delivered_date_month_avg_5") <= 55))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "delivered_date_month",
        "carrier_filled",
        "first_known_cost",
        "delivered_date_month_bucket",
        "delivered_date_month_avg_5",
        F.date_format("delivered_date", "MMM yyyy").alias("delivered_date_label"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled_2"),
    )
)

result = (
    cleaned_shipments
    .groupBy(F.date_trunc("YEAR", "delivered_date").alias("year_start"), "order_id")
    .agg(
        F.avg(F.when(F.col("carrier") == "UPS", F.col("delivered_date_month"))).alias("ups_avg_delivered_date_month"),
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("delivered_date_month")).otherwise(0)).alias("kerry_delivered_date_month"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
    )
)
