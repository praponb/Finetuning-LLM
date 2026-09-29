from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-5, -1)
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.min("revenue").alias("min_store_sale_revenue"))

enriched = (
    df_store_sales
    .select(
        "*",
        F.count("revenue").over(w).alias("product_id_count_revenue"),
        F.corr("revenue", "units").over(w).alias("corr_revenue_units"),
        F.avg("revenue").over(w2).alias("revenue_avg_3"),
        F.sum("units").over(w3).alias("cumulative_units"),
        F.avg("revenue").over(w4).alias("revenue_avg_5_prior"),
    )
)

result = (
    enriched
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "product_id_count_revenue",
        "revenue_avg_3",
        "cumulative_units",
        F.col("min_store_sale_revenue"),
    )
    .orderBy(F.col("id").desc_nulls_first())
    .limit(20)
)
