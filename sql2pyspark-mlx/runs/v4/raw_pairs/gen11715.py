from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w2 = Window.partitionBy("order_id", "carrier").orderBy(F.col("shipped_date").desc(), "id")
w3 = Window.partitionBy("quarter_start").orderBy(F.col("cost_p90").desc())

scored = (
    df_shipments
    .select(
        "*",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("QUARTER", "delivered_date").alias("quarter_start"),
        F.date_trunc("MONTH", "shipped_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.max(F.when(F.col("status") == "RETURNED", F.col("cost"))).alias("returned_max_cost"),
        F.min("next_cost").alias("min_next_cost"),
    )
    .withColumn("cost_p90_percent_rank", F.percent_rank().over(w3))
)
