from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc_nulls_first())

base = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-07-06").cast("date"))
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.substring("brand", 1, 4).alias("brand_prefix"),
        F.add_months("launch_date", 3).alias("launch_date_plus_months"),
    )
)

result = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)
