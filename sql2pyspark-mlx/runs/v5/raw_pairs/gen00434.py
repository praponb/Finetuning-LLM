from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
w2 = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_last())
w3 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("product_id").orderBy("units", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([3, 5, 7]))
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        F.percent_rank().over(w).alias("revenue_percent_rank"),
        F.percent_rank().over(w2).alias("units_percent_rank"),
        F.avg("units").over(w3).alias("units_avg_5_prior"),
        F.lead("revenue", 1).over(w4).alias("next_revenue"),
        F.ntile(5).over(w5).alias("units_bucket"),
    )
)
