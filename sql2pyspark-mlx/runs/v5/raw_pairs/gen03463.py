from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc())

result = (
    df_products
    .filter(F.col("brand") == "Stark")
    .select(
        "id",
        "category",
        "cost",
        "launch_date",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 5)
    .orderBy("launch_date")
)
