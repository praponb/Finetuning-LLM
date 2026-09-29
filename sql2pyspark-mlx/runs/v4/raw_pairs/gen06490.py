from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("quantity") >= 207)
    .select(
        "id",
        "product_id",
        "store_id",
        "quantity",
        F.percent_rank().over(w).alias("quantity_pct_rank"),
    )
)
