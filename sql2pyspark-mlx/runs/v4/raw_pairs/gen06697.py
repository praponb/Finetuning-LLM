from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id", "product_id")
w2 = Window.orderBy(F.col("units").desc(), "id")
w_grp = Window.partitionBy("store_id")
w4 = Window.partitionBy("product_id").orderBy("least_units_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("cumulative_units", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("store_id_sum_revenue", "id")

filtered = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2024-03-27"))) & (F.col("revenue") >= 3800))
    .select("*", F.least("units", "revenue").alias("least_units_revenue"))
)

metrics = (
    filtered
    .select(
        "*",
        F.sum("revenue").over(w_hist).alias("store_id_sum_revenue"),
        F.ntile(10).over(w2).alias("units_bucket"),
        F.corr("units", "revenue").over(w_grp).alias("corr_units_revenue"),
        F.count("least_units_revenue").over(w_grp).alias("store_id_count_least_units_revenue"),
        F.sum("units").over(w4).alias("cumulative_units"),
    )
)

scored = (
    metrics
    .withColumn("corr_units_revenue_avg_6", F.avg("corr_units_revenue").over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
)

result = (
    scored
    .select("id", "store_id", "least_units_revenue", "revenue", "corr_units_revenue")
    .unpivot(
        ["id", "store_id"],
        ["least_units_revenue", "revenue", "corr_units_revenue"],
        "metric",
        "value",
    )
)
