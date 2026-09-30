from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")
w3 = Window.orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("seq_no", "id").rowsBetween(-6, -1)

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("plan") == "basic"))
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "plan_title",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
    )
)

base = (
    metrics
    .filter(F.col("seq_no").isNotNull())
    .withColumn("seq_no_2_band", F.when(F.col("seq_no_2") >= 8, "High").when(F.col("seq_no_2") >= 4, "Medium").otherwise("Low"))
)

base_subscriptions = (
    base
    .filter(F.col("seq_no") != 6)
    .withColumn("seq_no_adjusted", F.round((F.col("seq_no") * 1.07), 2))
)

scored = (
    base_subscriptions
    .filter(F.col("seq_no") > 6)
    .select(
        "id",
        "seq_no",
        "seq_no_adjusted",
        F.sum("seq_no_adjusted").over(w3).alias("running_seq_no_adjusted"),
        F.avg("seq_no").over(w_hist).alias("seq_no_avg_6_prior"),
    )
)

result = (
    scored
    .select("id", "seq_no_adjusted", "running_seq_no_adjusted", "seq_no_avg_6_prior")
    .orderBy(F.col("running_seq_no_adjusted").desc_nulls_last())
)
