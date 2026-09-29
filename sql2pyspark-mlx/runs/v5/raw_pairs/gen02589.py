from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").asc_nulls_first(), "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("units").alias("min_store_sale_units"))

latest = (
    df_store_sales
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "row_num",
        F.col("min_store_sale_units"),
    )
    .orderBy(F.col("units").desc())
)
