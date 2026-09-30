from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "TH"]))
    .groupBy("country")
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("pct_of_total", F.round(((F.col("total_rating") * 100) / F.sum("total_rating").over(w)), 2))
    .orderBy(F.col("country").desc(), F.col("total_rating").desc())
)
