from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("order_id").orderBy("cost", "id")
w3 = Window.orderBy("greatest_cost_filled_cost", "id")
w_running = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_first())
w5 = Window.partitionBy("order_id").orderBy("cost_filled", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "DHL"]))
    .select(
        "*",
        F.date_trunc("year", "delivered_date").alias("delivered_date_period"),
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

cleaned = (
    valid
    .filter((F.col("shipped_date") >= F.lit("2023-04-15").cast("date")) & (F.col("shipped_date") <= "2023-11-01"))
    .select("*", F.greatest("cost_filled", "cost").alias("greatest_cost_filled_cost"))
)

scored = (
    cleaned
    .withColumn("cost_filled_avg_5_prior", F.avg("cost_filled").over(w))
    .withColumn("cost_bucket", F.ntile(5).over(w2))
    .withColumn("prev_greatest_cost_filled_cost", F.lag("greatest_cost_filled_cost", 1).over(w3))
    .withColumn("cost_cume_dist", F.cume_dist().over(w_running))
    .withColumn("cost_filled_avg_6", F.avg("cost_filled").over(w5))
)

result = (
    scored
    .select(
        "id",
        "carrier",
        "cost",
        F.col("cost_cume_dist").alias("COST_CUME_DIST"),
        F.col("prev_greatest_cost_filled_cost").alias("PREV_GREATEST_COST_FILLED_COST"),
    )
    .unpivot(
        ["id", "carrier"],
        ["cost", "COST_CUME_DIST", "PREV_GREATEST_COST_FILLED_COST"],
        "metric",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
