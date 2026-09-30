from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating")

latest = (
    df_suppliers
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "supplier_name", "rating")
)

result = (
    latest
    .select("id", "supplier_name", "rating")
    .orderBy(F.col("id").desc())
    .limit(5)
)
