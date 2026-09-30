from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("cost_scaled").desc_nulls_first())

filtered = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-10-05").cast("date")) & F.col("carrier").isNotNull() & (F.year("delivered_date") == 2025))
    .withColumn("cost_rounded", F.round("cost", 0))
    .withColumn("cost_scaled", F.round((F.col("cost") * 0.85), 2))
)

result = (
    filtered
    .select(
        "*",
        F.first("carrier", ignorenulls=True).over(w).alias("first_known_carrier"),
        F.dense_rank().over(w2).alias("cost_scaled_dense_rank"),
    )
)
