from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_first(), "id")

latest = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 4400, "PREMIUM").otherwise("STANDARD"))
    .cube("revenue_tier")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.count("*").alias("cnt"),
        F.avg("revenue").alias("avg_revenue_2"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("revenue_tier").alias("gid"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("avg_revenue_2").asc_nulls_last())
)
