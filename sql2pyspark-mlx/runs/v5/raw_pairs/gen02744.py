from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last())

cleaned = (
    df_suppliers
    .filter(F.col("rating").between(3.4, 3.6))
    .withColumn("supplier_name_upper", F.upper("supplier_name"))
    .withColumn("country_length", F.length("country"))
)

result = (
    cleaned
    .select(
        "id",
        "supplier_name",
        "country",
        "supplier_name_upper",
        F.avg("country_length").over(w).alias("running_avg_country_length"),
        F.max("country_length").over(w).alias("running_max_country_length"),
        F.min("country_length").over(w).alias("running_min_country_length"),
        F.cume_dist().over(w2).alias("rating_cume_dist"),
    )
)
