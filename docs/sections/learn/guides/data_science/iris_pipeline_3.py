import dagstermill as dm

from dagster import DependencyDefinition, InputDefinition, Path, PipelineDefinition
from dagster.utils import script_relative_path
from dagster.utils.download import download_file


k_means_iris_solid = dm.define_dagstermill_solid(
    'k_means_iris',
    script_relative_path('iris-kmeans_3.ipynb'),
    inputs=[InputDefinition('path', Path, description='Local path to the Iris dataset')]
)

prediction_iris_solid = dm.define_dagstermill_solid(
    'prediction_iris',
    script_relative_path('iris-predictions_2.ipynb'),
    inputs=[InputDefinition('path', Path, description='Local path to the Iris dataset')]
)

def define_iris_pipeline():
    return PipelineDefinition(
        name='iris_pipeline',
        solids=[download_file, k_means_iris_solid, prediction_iris_solid],
        dependencies={
            'k_means_iris': {
                'path': DependencyDefinition('download_file', 'path')
            },
            'prediction_iris': {
                'path': DependencyDefinition('download_file', 'path')
            }
        }
    )
