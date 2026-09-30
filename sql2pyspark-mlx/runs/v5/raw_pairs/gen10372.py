from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("revenue").between(1300, 2500) & F.col("product_id").isin([1, 3, 5, 7]))
)

summary = (
    src
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.sum("units").alias("total_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = (
    summary
    .select("sale_quarter", "total_units", "avg_revenue")
    .orderBy("avg_revenue", F.col("total_units").desc())
)
