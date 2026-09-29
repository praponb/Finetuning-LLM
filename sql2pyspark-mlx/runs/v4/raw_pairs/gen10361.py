from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft")

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.initcap(F.trim("store_name")).alias("store_name_title"),
    )
)

result = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
