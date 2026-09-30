from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & (~F.col("country").isin(["DE", "SG", "TH"]) | ~F.col("city").isin(["London", "Berlin"])))
    .withColumn("city_upper", F.upper("city"))
    .withColumn("city_lower", F.lower("city"))
    .orderBy("rating", "city_lower")
)
