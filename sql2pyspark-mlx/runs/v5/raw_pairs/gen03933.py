from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("city").isin(["London", "Chiang Mai"]))
    .select(
        "id",
        "country",
        "rating",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.ntile(4).over(w2).alias("quartile"),
    )
)
