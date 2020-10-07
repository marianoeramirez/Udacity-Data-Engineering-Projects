from airflow.plugins_manager import AirflowPlugin

from . import operators
from . import helpers


class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.CreateTableOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
