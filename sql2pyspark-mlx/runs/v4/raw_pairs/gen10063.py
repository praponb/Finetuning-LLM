from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id")
w2 = Window.partitionBy("segment", "city").orderBy("age", "id")
w3 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("signup_date", "id")
w5 = Window.partitionBy("city", "segment").orderBy("credit_limit", "id").rowsBetween(-5, -1)

filtered = (
    df_customers
    .filter(F.col("age") <= 31)
    .select(
        "*",
        F.concat("name", F.lit(" - "), "country").alias("name_country"),
        F.coalesce("segment", F.lit("unassigned")).alias("segment_filled"),
    )
)

cleaned = (
    filtered
    .filter(F.col("city") == "New York")
    .select(
        "*",
        F.round((F.col("credit_limit") * 0.85), 2).alias("adjusted_credit_limit"),
        F.coalesce("segment", F.lit("NONE")).alias("resolved_segment"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.ntile(5).over(w).alias("age_bucket"),
        F.row_number().over(w2).alias("event_seq"),
        F.avg("age").over(w3).alias("age_avg_5"),
        F.lag("credit_limit", 1).over(w4).alias("prev_credit_limit"),
        F.avg("age").over(w5).alias("age_avg_5_prior"),
    )
)

base = (
    scored
    .filter(F.col("signup_date") >= F.lit("2023-11-02").cast("date"))
    .withColumn("age_level", F.when(F.col("age") >= 58, "High").when(F.col("age") >= 32, "Medium").otherwise("Low"))
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
    .withColumn("city_clean", F.coalesce("city", F.lit("DIRECT")))
)

result = (
    base
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "age",
        "name_country",
        "segment_filled",
        F.when(F.col("name").rlike("^(Emma Brown|Noah Park)$"), "GROUP_B").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "GROUP_A").otherwise("PRIORITY").alias("name_zone"),
    )
    .orderBy("age", F.col("segment").desc())
)
