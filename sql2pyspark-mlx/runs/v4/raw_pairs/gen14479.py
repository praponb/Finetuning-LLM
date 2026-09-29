from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")
w3 = Window.orderBy("budget", "id").rowsBetween(-3, -1)
w_ordered = Window.partitionBy("location").orderBy("budget", "id")
w_running = Window.partitionBy("dept_name")
w6 = Window.partitionBy("location")
w7 = Window.partitionBy("dept_name", "location").orderBy("next_budget_2", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("dept_name").orderBy(F.col("dept_name_max_budget").asc_nulls_first())
w9 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

enriched = (
    df_departments
    .select(
        "*",
        F.lead("budget", 1).over(w).alias("next_budget"),
        F.ntile(4).over(w2).alias("budget_bucket"),
        F.avg("budget").over(w3).alias("budget_avg_3_prior"),
        F.lead("budget", 2).over(w_ordered).alias("next_budget_2"),
        F.max("budget").over(w_running).alias("dept_name_max_budget"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.round((F.col("next_budget_2") / F.sum("next_budget_2").over(w6)), 4).alias("next_budget_2_share"),
        F.avg("next_budget_2").over(w7).alias("next_budget_2_avg_5_prior"),
        F.percent_rank().over(w8).alias("dept_name_max_budget_percent_rank"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .groupBy("location")
    .pivot("dept_name", ["Marketing", "Finance", "Legal"])
    .agg(F.avg("budget_avg_3_prior").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "location",
        F.col("Marketing_value_sum").alias("marketing_value_sum"),
        F.col("Marketing_n").alias("marketing_n"),
        F.col("Finance_value_sum").alias("finance_value_sum"),
        F.col("Finance_n").alias("finance_n"),
        F.col("Legal_value_sum").alias("legal_value_sum"),
        F.col("Legal_n").alias("legal_n"),
    )
)
