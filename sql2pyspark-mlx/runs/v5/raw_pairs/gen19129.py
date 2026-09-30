from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("days_since_created_at").desc_nulls_last())
w_ordered = Window.partitionBy("element").orderBy(F.col("days_since_created_at").asc_nulls_last())
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("element"))

valid = (
    exploded
    .filter((F.col("created_at") >= F.lit("2025-03-01 15:00:00").cast("timestamp")) & F.hour("created_at").between(9, 16))
    .withColumn("element_upper", F.upper("element"))
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
    .withColumn("element_filled", F.coalesce("element", F.lit("NONE")))
)

cleaned = (
    valid
    .filter(F.col("element").isNotNull() & (F.col("days_since_created_at") >= 31))
    .select("*", F.to_date("created_at").alias("created_at_date"))
)

scored = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w).alias("days_since_created_at_cume_dist"),
        F.rank().over(w_ordered).alias("days_since_created_at_rank"),
        F.sum("days_since_created_at").over(w3).alias("days_since_created_at_sum_15m"),
        F.max("days_since_created_at").over(w4).alias("days_since_created_at_max_1h"),
        F.avg("days_since_created_at").over(w5).alias("days_since_created_at_avg_15m"),
    )
)

result = (
    scored
    .select(
        "id",
        "created_at",
        "days_since_created_at",
        "element_filled",
        "created_at_date",
        "days_since_created_at_max_1h",
        "days_since_created_at_avg_15m",
    )
)
