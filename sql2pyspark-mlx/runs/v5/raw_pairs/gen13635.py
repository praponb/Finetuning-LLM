from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "DE")
    .withColumn("city_upper", F.upper("city"))
    .withColumn("country_group", F.when(F.col("country").isin(["UK", "DE"]), "Priority").otherwise("Other"))
)
