from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id").orderBy("units", "id")
w3 = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-6, -1)

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-08-02").cast("date"))
    .withColumn("sale_date_weekofyear", F.weekofyear("sale_date"))
    .withColumn("revenue_per_unit", (F.col("revenue") + F.col("units")))
)

result = (
    filtered
    .select(
        "*",
        F.stddev_samp("sale_date_weekofyear").over(w).alias("store_id_stddev_samp_sale_date_weekofyear"),
        F.ntile(3).over(w2).alias("units_bucket"),
        F.avg("units").over(w3).alias("units_avg_6_prior"),
    )
)
