FROM jupyter/datascience-notebook
ADD ./data/movielens /home/jovyan/movielens_v2/movielens/
ADD ./data/Elasticsearch.ipynb /home/jovyan/
ADD ./data/Elasticsearch-AssignmentTemplate.ipynb /home/jovyan/
RUN pip install elasticsearch
RUN pip install pandas