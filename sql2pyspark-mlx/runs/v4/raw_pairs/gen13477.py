from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("product_id").orderBy("revenue", "id")

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-08-21").cast("date")) & (F.col("revenue") >= 1000))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)

result = (
    cleaned
    .withColumn("revenue_avg_3_prior", F.avg("revenue").over(w))
    .withColumn("prev_revenue", F.lag("revenue", 2, 0).over(w2))
)
