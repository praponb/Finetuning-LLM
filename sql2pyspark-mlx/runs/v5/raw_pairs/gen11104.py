from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "UK"]))
    .select("id", "country", "rating", F.row_number().over(w).alias("rn"))
    .orderBy(F.col("rating").desc(), "id")
)
