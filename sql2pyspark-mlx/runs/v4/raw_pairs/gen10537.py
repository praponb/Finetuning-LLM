from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

base = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Glo%"))
    .select(
        "*",
        F.ceil("rating").alias("rating_whole"),
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
    )
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
