from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-09-23")))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.round((F.col("quantity") * 100), 2).alias("adjusted_quantity"),
    )
)

result = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
