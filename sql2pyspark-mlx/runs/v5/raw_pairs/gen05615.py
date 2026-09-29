from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id")

latest = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy("category")
    .agg(
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.max(F.when(F.col("brand") == "Stark", F.col("price"))).alias("stark_max_price"),
    )
    .filter(F.col("stock_qty_bit_or") < 1000)
    .orderBy(F.col("stark_max_price").desc_nulls_first())
)
