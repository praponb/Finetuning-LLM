from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_last(), "id")
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.sum("revenue").alias("sum_store_sale_revenue"))

latest = (
    df_store_sales
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "row_num",
        F.col("sum_store_sale_revenue"),
    )
    .orderBy("sum_store_sale_revenue")
)
