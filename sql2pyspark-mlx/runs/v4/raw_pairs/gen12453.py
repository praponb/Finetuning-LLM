from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_store_sales
    .select(
        "*",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 4).alias("revenue_share"),
        F.sum("revenue").over(w2).alias("cumulative_revenue"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("revenue_share") >= 0.67, "HIGH").otherwise("LOW").alias("revenue_share_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.bit_and("units").alias("units_bit_and"),
    )
    .orderBy(F.col("units_bit_and").desc_nulls_last())
)
