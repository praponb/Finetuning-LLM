from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-08-16").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("Kerry"))))
    .select(
        "*",
        F.dayofmonth("delivered_date").alias("delivered_date_day"),
        F.round((F.col("cost") * 0.85), 2).alias("adjusted_cost"),
    )
)

result = (
    cleaned
    .select("status", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("cost") >= 47)
        .select("status", "order_id")
    )
)
