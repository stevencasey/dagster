import dagstermill as dm

from dagster import PipelineDefinition
from dagster.utils import script_relative_path

k_means_iris_solid = dm.define_dagstermill_solid(
    'k_means_iris',
    script_relative_path('iris-kmeans.ipynb'),
)

def define_iris_pipeline():
    return PipelineDefinition(
        name='iris_pipeline',
        solids=[k_means_iris_solid],
    )
