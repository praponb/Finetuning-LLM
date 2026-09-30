from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("country") == "DE", 1).otherwise(0).alias("is_de"),
        F.to_date("started_at").alias("started_at_date"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.email"))
)

base_sessions = (
    enriched
    .filter(F.col("email").like("%@gmail.com"))
    .select(
        "*",
        F.add_months("started_at_date", 6).alias("started_at_date_plus_months"),
        F.concat("browser", F.lit(" - "), "country").alias("browser_country"),
        F.coalesce("browser", F.lit("NONE")).alias("resolved_browser"),
    )
)

result = (
    base_sessions
    .groupBy("country")
    .pivot("browser", ["Safari", "Edge", "Chrome"])
    .agg(F.avg("is_de").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "country",
        F.col("Safari_total").alias("safari_total"),
        F.col("Safari_cnt").alias("safari_cnt"),
        F.col("Edge_total").alias("edge_total"),
        F.col("Edge_cnt").alias("edge_cnt"),
        F.col("Chrome_total").alias("chrome_total"),
        F.col("Chrome_cnt").alias("chrome_cnt"),
    )
)
