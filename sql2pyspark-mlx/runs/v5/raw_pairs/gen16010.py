from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy("customer_country_2").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("country").isNotNull() & (~F.col("browser").eqNullSafe(F.lit("Firefox"))))
    .withColumn("browser_upper", F.upper("browser"))
    .withColumn("country_clean", F.coalesce("country", F.lit("DIRECT")))
)

enriched = (
    cleaned
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.*",
        F.col("p.country").alias("customer_country"),
        F.col("p.city"),
        F.col("p.signup_date"),
    )
)

enriched_sessions = (
    enriched
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.browser",
        "a.country",
        "a.browser_upper",
        "a.city",
        "a.signup_date",
        F.col("b.country").alias("customer_country_2"),
    )
)

metrics = (
    enriched_sessions
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w2))
    .withColumn("first_known_customer_country_2", F.first("customer_country_2", ignorenulls=True).over(w2))
)

by_type = (
    metrics
    .groupBy("city", "country")
    .pivot("customer_country_2", ["TH", "US", "DE"])
    .agg(F.sum("event_seq"))
    .select(
        "city",
        "country",
        F.col("TH").alias("th"),
        F.col("US").alias("us"),
        F.col("DE").alias("de"),
    )
)

result = (
    by_type
    .select(
        "city",
        "country",
        "th",
        "de",
        F.when(F.col("city").rlike("^(New York|Tokyo)$"), "EXTENDED").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy("city")
)
