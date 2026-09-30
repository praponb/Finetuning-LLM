from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id")

scored = (
    df_store_sales
    .select(
        "*",
        F.first("revenue", ignorenulls=True).over(w).alias("first_known_revenue"),
        F.corr("revenue", "units").over(w2).alias("corr_revenue_units"),
    )
)

result = (
    scored
    .groupBy(
        "product_id",
        F.when(F.col("corr_revenue_units") >= 0.46, "HIGH").otherwise("LOW").alias("corr_revenue_units_tier"),
    )
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("first_known_revenue").alias("total_first_known_revenue"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
    )
    .orderBy(F.col("corr_revenue_units_tier").desc_nulls_first())
)
