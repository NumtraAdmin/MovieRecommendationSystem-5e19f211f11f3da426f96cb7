import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e19f211f11f3da426f96cb8','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	MovieRecommendationSystem_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e19f211f11f3da426f96cb8", spark, "{'url': '/Demo/MovieRatingsTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi743e2d3cc92a32916f8c2fa9bd7d0606', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e19f211f11f3da426f96cb8','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e19f211f11f3da426f96cb8','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e19f211f11f3da426f96cb9','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	MovieRecommendationSystem_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e19f211f11f3da426f96cb8"],{"5e19f211f11f3da426f96cb8": MovieRecommendationSystem_DBFS}, "5e19f211f11f3da426f96cb9", spark,json.dumps( {"FE": [{"feature": "UserId", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "471.95", "stddev": "256.89", "min": "2", "max": "938", "missing": "0"}}, {"feature": "MovieId", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "477.42", "stddev": "354.51", "min": "2", "max": "1649", "missing": "0"}}, {"feature": "Rating", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "1000", "mean": "3.47", "stddev": "1.11", "min": "1.0", "max": "5.0", "missing": "0"}}, {"transformationsData": {"feature_label": "Timestamp"}, "feature": "Timestamp", "transformation": "Extract Date", "type": "date", "selected": "True", "stats": {"count": "", "mean": "", "stddev": "", "min": "", "max": "", "missing": "0"}}, {"feature": "AvgRating", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "1000", "mean": "3.48", "stddev": "0.44", "min": "1.8", "max": "4.5", "missing": "0"}}, {"feature": "Timestamp_dayofmonth", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "16.49", "stddev": "8.48", "min": "1", "max": "31", "missing": "0"}}, {"feature": "Timestamp_month", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "6.71", "stddev": "4.43", "min": "1", "max": "12", "missing": "0"}}, {"feature": "Timestamp_year", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "1997.47", "stddev": "0.5", "min": "1997", "max": "1998", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e19f211f11f3da426f96cb9','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e19f211f11f3da426f96cb9','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e19f211f11f3da426f96cba','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	MovieRecommendationSystem_AutoML = tpot_execution.Tpot_execution.run(["5e19f211f11f3da426f96cb9"],{"5e19f211f11f3da426f96cb9": MovieRecommendationSystem_AutoFE}, "5e19f211f11f3da426f96cba", spark,json.dumps( {"model_type": "regression", "label": "AvgRating", "features": ["UserId", "MovieId", "Rating", "Timestamp", "Timestamp_dayofmonth", "Timestamp_month", "Timestamp_year"], "percentage": "20", "executionTime": 5, "sampling": "0", "sampling_value": "", "run_id": "", "model_id": "5e1a1b771bfdaec91f38ae6c", "ProjectName": "Retail Scenarios", "PipelineName": "MovieRecommendationSystem", "userid": "567a95c8ca676c1d07d5e3e7", "runid": "", "url_ResultView": "http://104.40.91.74:3200", "experiment_id": "895518857185768"}))

	PipelineNotification.PipelineNotification().completed_notification('5e19f211f11f3da426f96cba','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e19f211f11f3da426f96cba','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)

