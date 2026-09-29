from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("country").isin(["TH", "JP"]) & (F.col("country") == "TH"))
    .withColumn("country_browser", F.concat("country", F.lit(", "), "browser"))
    .withColumn("started_at_label", F.date_format("started_at", "MMM yyyy"))
)
