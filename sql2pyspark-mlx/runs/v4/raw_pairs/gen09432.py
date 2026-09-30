from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Safari", "Firefox"]) & F.col("customer_id").isin([23, 28]) & F.col("browser").isNull())
    .withColumn("country_upper", F.upper("country"))
)
