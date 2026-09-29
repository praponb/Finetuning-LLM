from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost_scaled", "id")
w2 = Window.partitionBy("carrier").orderBy("is_in_transit", "id")
w3 = Window.partitionBy("carrier").orderBy("cost_scaled", "id")

cleaned = (
    df_shipments
    .filter(F.col("delivered_date") > F.to_date(F.lit("2023-08-21")))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0).alias("is_in_transit"),
        F.round((F.col("cost") * 0.9), 2).alias("cost_scaled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        "cost_scaled",
        F.lead("cost_scaled", 1).over(w).alias("next_cost_scaled"),
        F.lag("is_in_transit", 2).over(w2).alias("prev_is_in_transit"),
        F.ntile(4).over(w3).alias("cost_scaled_bucket"),
    )
)
