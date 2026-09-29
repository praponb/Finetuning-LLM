from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "customer_id").orderBy(F.col("started_at_period").desc(), "id")

filtered = (
    df_sessions
    .filter(~F.col("browser").isin(["Firefox", "Edge", "Safari"]))
    .select("*", F.date_trunc("MONTH", "started_at").alias("started_at_period"))
)

cleaned = (
    filtered
    .filter((F.col("started_at") >= F.lit("2023-03-25 06:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        "started_at_period",
        F.concat("country", F.lit(" - "), "device").alias("country_device"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .select("id", "browser", "country", "country_device")
    .orderBy(F.col("id").desc(), "browser")
)
