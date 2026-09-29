from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status")
w2 = Window.partitionBy("status").orderBy(F.col("delivered_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("delivered_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost_avg_30d").asc_nulls_last())
w5 = Window.partitionBy("order_id", "carrier").orderBy(F.col("delivered_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)

base = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-03-03", "2024-03-19"))
    .select("*", F.date_trunc("YEAR", "delivered_date").alias("delivered_date_period"))
)

remaining_keys = (
    base
    .select("id", "carrier")
    .intersect(
        df_shipments
        .filter(F.col("delivered_date").between("2025-09-02", "2025-10-01"))
        .select("id", "carrier")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

matched_keys = (
    verified
    .select("status", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("order_id") == 29)
        .select("status", "carrier")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .withColumn("carrier_count_cost", F.count("cost").over(w))
    .withColumn("cost_avg_30d", F.avg("cost").over(w2))
    .withColumn("cost_count_30d", F.count("cost").over(w3))
)

scored = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w4).alias("cost_avg_30d_cume_dist"),
        F.count("cost_count_30d").over(w5).alias("cost_count_30d_count_7d"),
    )
)

valid = (
    scored
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "delivered_date_period",
        "carrier_count_cost",
        "cost_avg_30d_cume_dist",
        "cost_count_30d_count_7d",
        F.greatest("carrier_count_cost", "cost_avg_30d_cume_dist").alias("greatest_carrier_count_cost_cost_avg_30d_cume_dist"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("cost_avg_30d_cume_dist") >= 0.69, "LARGE").otherwise("SMALL").alias("cost_avg_30d_cume_dist_tier"),
        F.date_trunc("MONTH", "shipped_date").alias("activity_month"),
    )
    .agg(
        F.avg("cost_count_30d_count_7d").alias("avg_cost_count_30d_count_7d"),
        F.round(F.sum("greatest_carrier_count_cost_cost_avg_30d_cume_dist") / F.nullif(F.count(F.when(F.col("carrier") == "FedEx", 1)), F.lit(0)), 2).alias("greatest_carrier_count_cost_cost_avg_30d_cume_dist_per_fedex"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("avg_cost_count_30d_count_7d") >= 0)
)
