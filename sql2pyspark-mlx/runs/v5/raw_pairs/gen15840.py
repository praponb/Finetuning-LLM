from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("days_delivered_date_to_shipped_date").desc(), "id")
w2 = Window.partitionBy("order_id", "status").orderBy(F.col("days_delivered_date_to_shipped_date_rounded").desc_nulls_first(), "id")

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("Flash"))))
    .select(
        "*",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

filtered = (
    cleaned
    .filter(F.col("carrier").isNotNull() & F.col("delivered_date").between("2023-04-27", "2023-08-31"))
    .select(
        "*",
        F.coalesce("days_delivered_date_to_shipped_date", F.lit(0)).alias("days_delivered_date_to_shipped_date_filled"),
        F.round("days_delivered_date_to_shipped_date", 2).alias("days_delivered_date_to_shipped_date_rounded"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

ranked = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

summary = (
    ranked
    .groupBy(F.date_trunc("year", "delivered_date").alias("year_start"))
    .agg(
        F.round(F.avg("days_delivered_date_to_shipped_date_rounded"), 2).alias("avg_days_delivered_date_to_shipped_date_rounded"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("carrier").alias("distinct_carriers"),
    )
)

result = (
    summary
    .select("year_start", "status_list", "distinct_carriers")
    .orderBy(F.col("year_start").asc_nulls_last())
    .limit(20)
)
