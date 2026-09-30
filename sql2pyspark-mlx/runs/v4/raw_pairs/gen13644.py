from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").asc_nulls_last(), "id")

deduped = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select("id", "supplier_name", "country", "city", "rn")
    .orderBy(F.col("country").desc_nulls_last())
)
