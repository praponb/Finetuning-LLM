from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "SMB", "Corporate"]))
    .select("id", "country", "age", F.ntile(3).over(w).alias("quartile"))
    .orderBy(F.col("country").desc())
)
