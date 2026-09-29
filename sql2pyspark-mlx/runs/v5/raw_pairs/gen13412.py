from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        "sale_date",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "product_id", "revenue", "sale_date")
)
