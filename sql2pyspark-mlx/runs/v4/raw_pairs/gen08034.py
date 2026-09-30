from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "country",
        "rating",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
