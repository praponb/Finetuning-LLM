from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("clean_city", F.trim("city"))
    .withColumn("country_upper", F.upper("country"))
)
