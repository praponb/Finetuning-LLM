from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(-6, -1)

with_windows = (
    df_store_sales
    .select(
        "*",
        F.avg("revenue").over(w).alias("revenue_avg_5_prior"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share"),
        F.sum("revenue").over(w3).alias("cumulative_revenue"),
        F.avg("units").over(w4).alias("units_avg_6_prior"),
    )
)

base = (
    with_windows
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-07-02"))) & (F.col("units") == 16))
    .withColumn("revenue_avg_5_prior_scaled", F.round((F.col("revenue_avg_5_prior") * 1.07), 2))
    .withColumn("cumulative_revenue_str", F.col("cumulative_revenue").cast("double"))
)

joined = (
    base
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.price"), F.col("b.product_name"), F.col("b.brand"))
)

result = (
    joined
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "cumulative_revenue",
        "units_avg_6_prior",
        "cumulative_revenue_str",
        "price",
    )
)
