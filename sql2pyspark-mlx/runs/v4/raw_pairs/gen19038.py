from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "US"]))
    .select("id", "country", F.count("*").over(w).alias("country_count"))
    .orderBy(F.col("id").desc())
)
