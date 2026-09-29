from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Firefox", "Safari"]) & F.col("country").isin(["JP", "US"]))
    .withColumn("device_group", F.when(F.col("device").isin(["mobile", "tablet"]), "Tier 1").otherwise("Other"))
    .withColumn("browser_title", F.initcap(F.trim("browser")))
)
