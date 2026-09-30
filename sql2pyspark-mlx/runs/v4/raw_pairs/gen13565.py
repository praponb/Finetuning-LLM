from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .select(
        "id",
        "country",
        "rating",
        "supplier_name",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 1)
    .orderBy("rating")
)
