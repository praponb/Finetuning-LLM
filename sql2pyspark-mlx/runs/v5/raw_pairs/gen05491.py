from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("days_since_delivered_date").desc(), "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("carrier").orderBy("order_id_count_days_since_delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy("event_seq", "id")
w5 = Window.orderBy("order_id_count_days_since_delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("next_order_id_count_days_since_delivered_date_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("next_order_id_count_days_since_delivered_date_whole", "id").rowsBetween(-2, -1)

cleaned = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Flash", "Kerry"]))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
        F.lower("status").alias("status_lower"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.count("days_since_delivered_date").over(w2).alias("order_id_count_days_since_delivered_date"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "carrier",
        "cost",
        "days_since_delivered_date",
        "status_lower",
        F.sum("cost").over(w3).alias("cumulative_cost"),
        F.lead("order_id_count_days_since_delivered_date", 2).over(w4).alias("next_order_id_count_days_since_delivered_date"),
        F.first("days_since_delivered_date", ignorenulls=True).over(w5).alias("first_known_days_since_delivered_date"),
    )
)

cleaned_shipments = (
    metrics
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        (F.col("days_since_delivered_date") + F.col("first_known_days_since_delivered_date")).alias("days_since_delivered_date_plus_first_known_days_since_delivered_date"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

valid = (
    cleaned_shipments
    .filter(F.col("carrier").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("DHL"))))
    .select(
        "id",
        "cost",
        "days_since_delivered_date",
        "status_lower",
        "cumulative_cost",
        "next_order_id_count_days_since_delivered_date",
        "first_known_days_since_delivered_date",
        "carrier_clean",
        F.floor("next_order_id_count_days_since_delivered_date").alias("next_order_id_count_days_since_delivered_date_whole"),
        F.floor("days_since_delivered_date_plus_first_known_days_since_delivered_date").alias("days_since_delivered_date_plus_first_known_days_since_delivered_date_whole"),
    )
)

cleaned_shipments_2 = (
    valid
    .filter(F.col("first_known_days_since_delivered_date") >= 47)
    .select(
        "*",
        (F.col("days_since_delivered_date_plus_first_known_days_since_delivered_date_whole") + F.col("next_order_id_count_days_since_delivered_date")).alias("days_since_delivered_date_plus_first_known_days_since_delivered_date_whole_plus_next_order_id_count_days_since_delivered_date"),
        F.least("days_since_delivered_date_plus_first_known_days_since_delivered_date_whole", "next_order_id_count_days_since_delivered_date_whole").alias("least_days_since_delivered_date_plus_first_known_days_since_delivered_date_whole_next_order_id_count_days_since_delivered_date_whole"),
    )
)

enriched_shipments = (
    cleaned_shipments_2
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w6))
    .withColumn("cost_avg_2_prior", F.avg("cost").over(w7))
)

result = (
    enriched_shipments
    .select(
        "id",
        "days_since_delivered_date_plus_first_known_days_since_delivered_date_whole_plus_next_order_id_count_days_since_delivered_date",
        "last_known_cost",
        "least_days_since_delivered_date_plus_first_known_days_since_delivered_date_whole_next_order_id_count_days_since_delivered_date_whole",
    )
    .unpivot(
        ids=["id"],
        values=["days_since_delivered_date_plus_first_known_days_since_delivered_date_whole_plus_next_order_id_count_days_since_delivered_date", "last_known_cost", "least_days_since_delivered_date_plus_first_known_days_since_delivered_date_whole_next_order_id_count_days_since_delivered_date_whole"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
