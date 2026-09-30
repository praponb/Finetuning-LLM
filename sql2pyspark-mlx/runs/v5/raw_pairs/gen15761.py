from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & ~F.col("device").isin(["mobile", "desktop"]) & ~F.col("browser").isin(["Edge", "Chrome", "Safari"]))
    .select("device", "customer_id")
)
