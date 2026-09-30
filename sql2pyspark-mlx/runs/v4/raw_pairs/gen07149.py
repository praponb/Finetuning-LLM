from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_stores
    .groupBy(
        F.when(F.col("region").isin(["Central", "East"]), "core").otherwise("other").alias("region_group"),
    )
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("pct_of_total", F.round(((F.col("total_sqft") * 100) / F.sum("total_sqft").over(w)), 2))
)
