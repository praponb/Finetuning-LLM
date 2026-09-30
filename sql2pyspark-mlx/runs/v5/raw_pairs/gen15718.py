from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment", "country")
w4 = Window.partitionBy("country", "segment").orderBy("credit_limit", "id")
w5 = Window.partitionBy("country").orderBy("corr_age_credit_limit", "id")

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-07-07").cast("date")) & F.col("city").isNotNull())
    .withColumn("email_length", F.length("email"))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("OTHER")))
)

scored = (
    valid
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4))
    .withColumn("running_age", F.sum("age").over(w2))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w3))
    .withColumn("next_email_length", F.lead("email_length", 1).over(w4))
)

valid_customers = (
    scored
    .filter(F.col("age").between(41, 57))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "email_length",
        "running_age",
        "corr_age_credit_limit",
        "next_email_length",
        F.when(F.col("segment") == "Consumer", 1).otherwise(0).alias("is_consumer"),
    )
)

deduped = (
    valid_customers
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .cube(
        "country",
        F.when(F.col("corr_age_credit_limit") >= 0.65, "PREMIUM").otherwise("STANDARD").alias("corr_age_credit_limit_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
    .orderBy("avg_age")
)
