from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date_period").asc_nulls_last(), "id")
w2 = Window.orderBy("days_delivered_date_to_shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("days_delivered_date_to_shipped_date", "id")

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-05-03").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "*",
        F.concat("carrier", F.lit(" - "), "status").alias("carrier_status"),
        F.initcap(F.trim("carrier")).alias("carrier_title"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

valid = (
    cleaned
    .filter(F.col("carrier").isNotNull() & F.col("carrier").isin(["FedEx", "DHL", "UPS"]))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "carrier_title",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

cleaned_shipments = (
    valid
    .filter(F.col("carrier").isNotNull() & (F.col("carrier").eqNullSafe(F.lit("DHL"))))
    .withColumn("days_since_delivered_date", F.datediff(F.current_date(), "delivered_date"))
    .withColumn("carrier_upper", F.upper("carrier"))
    .withColumn("resolved_carrier_2", F.coalesce("carrier", F.lit("OTHER")))
)

valid_shipments = (
    cleaned_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
        F.date_trunc("YEAR", "delivered_date").alias("delivered_date_period"),
    )
)

latest = (
    valid_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "cost",
        "carrier_title",
        "days_delivered_date_to_shipped_date",
        "delivered_date_period",
    )
)

with_windows = (
    latest
    .select(
        "*",
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.lag("days_delivered_date_to_shipped_date", 1, 0).over(w_ordered).alias("prev_days_delivered_date_to_shipped_date"),
        F.ntile(5).over(w_ordered).alias("days_delivered_date_to_shipped_date_bucket"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "cost",
        "cumulative_cost",
        "days_delivered_date_to_shipped_date_bucket",
    )
    .orderBy(F.col("cumulative_cost").desc(), "cost")
)
