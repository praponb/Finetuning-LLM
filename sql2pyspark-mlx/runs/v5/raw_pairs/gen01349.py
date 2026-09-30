from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").asc_nulls_last())
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_first())

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .withColumn("days_shipped_date_to_delivered_date", F.datediff("delivered_date", "shipped_date"))
)

scored = (
    filtered
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "days_shipped_date_to_delivered_date",
        F.cume_dist().over(w).alias("cost_cume_dist"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
        F.count("days_shipped_date_to_delivered_date").over(w3).alias("order_id_count_days_shipped_date_to_delivered_date"),
        F.lag("cost", 2).over(w4).alias("prev_cost_2"),
        F.percent_rank().over(w5).alias("cost_percent_rank"),
    )
)

filtered_shipments = (
    scored
    .filter(F.col("carrier").isNotNull() & (F.col("delivered_date") > F.lit("2023-09-18").cast("date")))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "days_shipped_date_to_delivered_date",
        "cost_cume_dist",
        "prev_cost",
        "prev_cost_2",
        "cost_percent_rank",
        F.round((F.col("cost_percent_rank") * 1.15), 2).alias("adjusted_cost_percent_rank"),
        F.least("cost_cume_dist", "order_id_count_days_shipped_date_to_delivered_date").alias("least_cost_cume_dist_order_id_count_days_shipped_date_to_delivered_date"),
    )
)

joined = (
    filtered_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.channel"))
)

result = (
    joined
    .select("id", "order_id", "prev_cost", "adjusted_cost_percent_rank")
    .unpivot(
        ids=["id", "order_id"],
        values=["prev_cost", "adjusted_cost_percent_rank"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
