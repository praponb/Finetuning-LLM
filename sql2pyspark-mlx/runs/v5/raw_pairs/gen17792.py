from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id")
w2 = Window.partitionBy("status").orderBy("shipped_date_month_end", "id")

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-05-14"))) & F.col("cost").between(59, 81))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.substring("carrier", 1, 4).alias("carrier_prefix"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "order_id",
        "cost",
        "carrier_prefix",
        "carrier_clean",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.lag("cost", 2).over(w2).alias("prev_cost"),
    )
)
