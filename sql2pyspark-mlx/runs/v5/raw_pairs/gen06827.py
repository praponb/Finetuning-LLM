from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date")

result = (
    df_products
    .filter(F.col("price").between(380, 1190))
    .select(
        "id",
        "brand",
        "launch_date",
        F.percent_rank().over(w).alias("launch_date_pct_rank"),
    )
    .orderBy(F.col("id").desc(), F.col("launch_date").desc())
)
