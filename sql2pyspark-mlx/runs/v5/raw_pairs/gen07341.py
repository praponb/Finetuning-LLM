from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("carrier")

valid = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "IN_TRANSIT"]))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)

result = (
    valid
    .select(
        "*",
        F.max("cost").over(w).alias("last7_max_cost"),
        F.avg("cost").over(w).alias("last7_avg_cost"),
        F.min("cost").over(w).alias("last7_min_cost"),
        F.count("cost").over(w).alias("last7_count_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)
