from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_first())
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy(F.col("cost_rounded").desc())

base = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-11-11").cast("date")) & F.col("order_id").isNotNull() & (F.col("order_id") == 35))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        F.round("cost", 1).alias("cost_rounded"),
    )
)

result = (
    base
    .withColumn("cost_percent_rank", F.percent_rank().over(w))
    .withColumn("cumulative_cost_rounded", F.sum("cost_rounded").over(w2))
    .withColumn("cost_rounded_rank", F.rank().over(w3))
)
