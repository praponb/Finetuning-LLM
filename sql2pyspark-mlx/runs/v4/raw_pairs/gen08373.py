from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("status", "carrier").orderBy("shipped_date_plus_27d_month_end_plus_months_2", "id")

base = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-05-01"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.date_add("shipped_date", 27).alias("shipped_date_plus_27d"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("resolved_carrier"),
    )
)

cleaned = (
    base
    .filter(F.month("shipped_date_plus_27d").isin([4, 7, 8]))
    .withColumn("cost_rounded", F.round("cost", 0))
    .withColumn("shipped_date_plus_27d_month_end", F.last_day("shipped_date_plus_27d"))
)

valid = (
    cleaned
    .filter(F.col("cost").between(39, 71))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        "resolved_carrier",
        "cost_rounded",
        "shipped_date_plus_27d_month_end",
        F.add_months("shipped_date_plus_27d_month_end", 12).alias("shipped_date_plus_27d_month_end_plus_months"),
        F.add_months("shipped_date_plus_27d_month_end", 12).alias("shipped_date_plus_27d_month_end_plus_months_2"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_filled"),
    )
)

scored = (
    valid
    .withColumn("status_avg_cost", F.avg("cost").over(w))
    .withColumn("cost_bucket", F.ntile(10).over(w2))
    .withColumn("prev_cost_rounded", F.lag("cost_rounded", 2, 0).over(w3))
)

result = (
    scored
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        "shipped_date_plus_27d_month_end",
        "shipped_date_plus_27d_month_end_plus_months",
        "shipped_date_plus_27d_month_end_plus_months_2",
        "carrier_filled",
        F.when(F.col("status").rlike("^(IN_TRANSIT|RETURNED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(DELIVERED|LOST)$"), "EXTENDED").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("carrier").desc())
)
