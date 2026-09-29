from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_rating").desc())

result = (
    df_suppliers
    .filter(F.length("supplier_name") > 14)
    .groupBy(
        F.when(F.col("country").isin(["TH", "JP"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
)
