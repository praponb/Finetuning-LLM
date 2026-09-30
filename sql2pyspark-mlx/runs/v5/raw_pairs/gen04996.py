from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_last())
w_part = Window.partitionBy("carrier").orderBy("rank_in_group", "id")
w4 = Window.partitionBy("carrier", "order_id")
w5 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-08-25").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.concat("carrier", F.lit(" "), "status").alias("carrier_status"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

base = (
    filtered
    .filter(F.col("order_id").isNotNull() & (F.col("carrier").eqNullSafe(F.lit("FedEx"))))
    .withColumn("carrier_upper", F.upper("carrier"))
    .withColumn("carrier_clean_2", F.coalesce("carrier", F.lit("UNKNOWN")))
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    latest
    .filter(F.col("shipped_date") <= "2025-07-01")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "carrier_status",
        "carrier_clean",
        F.cume_dist().over(w2).alias("cost_cume_dist"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

with_windows = (
    scored
    .withColumn("carrier_min_cost_cume_dist", F.min("cost_cume_dist").over(w4))
    .withColumn("cost_bucket", F.ntile(10).over(w5))
)

result = (
    with_windows
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("activity_month"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_count"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("activity_month").asc_nulls_last())
)
