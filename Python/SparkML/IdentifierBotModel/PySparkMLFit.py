import argparse
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier

MODEL_PATH = 'spark_ml_model'
LABEL_COL = 'is_bot'


def model_params(rf):
    grid = ParamGridBuilder() \
        .addGrid(rf.maxDepth, [2, 3]) \
        .addGrid(rf.maxBins, [3, 4]) \
        .build()
    return grid


# ML models
def build_random_forest() -> RandomForestClassifier:
    rf = RandomForestClassifier(labelCol=LABEL_COL, featuresCol="features")
    return rf

def build_gbt_classifier() -> GBTClassifier:
    gbt = GBTClassifier(labelCol=LABEL_COL, featuresCol="features", maxIter=10)
    return gbt

def build_decision_tree() -> DecisionTreeClassifier:
    dt = DecisionTreeClassifier(labelCol=LABEL_COL, featuresCol="features")
    return dt




def vector_assembler() -> VectorAssembler:
    features = ['user_type_index', 'platform_index', 'duration', 'item_info_events',
                'select_item_events','make_order_events', 'events_per_min']
    va = VectorAssembler(inputCols=features, outputCol="features")
    return va


def build_evaluator() -> MulticlassClassificationEvaluator:
    evaluator = MulticlassClassificationEvaluator(labelCol=LABEL_COL,
                                                predictionCol="prediction",
                                                metricName="accuracy")
    return evaluator


def build_cv(rand_forest, evaluator, model_params) -> CrossValidator:
    cv = CrossValidator(estimator=rand_forest,
                        estimatorParamMaps=model_params,
                        evaluator=evaluator,
                        parallelism=2)
    return cv


    


def process(spark, data_path, model_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param data_path: путь до датасета
    :param model_path: путь сохранения обученной модели
    """
    train_df = spark.read.parquet(data_path)

    user_type_index = StringIndexer(inputCol='user_type', outputCol="user_type_index")
    platform_index = StringIndexer(inputCol='platform', outputCol="platform_index")

    assembler = vector_assembler()
    evaluator = build_evaluator()

    rf = build_random_forest()
    # gbt = build_gbt_classifier()
    # dt = build_decision_tree()

    pipeline = Pipeline(stages=[user_type_index, platform_index, assembler, rf])
    cv = build_cv(pipeline, evaluator, model_params(rf))

    models = cv.fit(train_df)
    best = models.bestModel
    predictions = best.transform(train_df)
    accuracy = evaluator.evaluate(predictions)

    print(f"Accuracy: {accuracy}")

    best.write().overwrite().save(model_path)






def main(data_path, model_path):
    spark = _spark_session()
    process(spark, data_path, model_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkMLFitJob').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, default='session-stat.parquet', help='Please set datasets path.')
    parser.add_argument('--model_path', type=str, default=MODEL_PATH, help='Please set model path.')
    args = parser.parse_args()
    data_path = args.data_path
    model_path = args.model_path
    main(data_path, model_path)
