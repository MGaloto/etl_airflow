import pandas as pd
from urllib import request
import gzip
from os import listdir
import os
import shutil

CUR_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_BASICS = 'title.basics'
DATA_RATINGS= 'title.ratings'
NAME_DATA_LOAD = 'movies'

class ETL():

    def _extract_data():
        with open( '{}/urls.txt'.format(CUR_DIR), 'r') as folder:
            lines = folder.read().splitlines()

        def _download_url(url, extension='.tsv.gz', extension_output='.csv'):
            print("Download data from: ", url)
            file_title = url.split('/')[-1]
            csvfile = file_title.replace(extension,'') + extension_output
            request.urlretrieve(url=url, filename=file_title)
            with gzip.open(file_title, 'rb') as data_in:
                with open( '{}/data_csv/{}'.format(CUR_DIR, csvfile), 'wb') as data_out:
                    shutil.copyfileobj(data_in, data_out)

        path_to_dir = CUR_DIR + '/data_csv'
        filenames = listdir(path_to_dir)
        csv_files = [file for file in filenames if file.endswith(".csv")]
        if not csv_files:
            for line in lines:
                if DATA_BASICS in line or DATA_RATINGS in line:
                    _download_url(line)
        else:
            print('Archivos CSV ya descargados: {}'.format(csv_files))


    def _transform_load_data():
        
        def _get_merge(data_uno, data_dos):
            print('Iniciando el merge del Data Frame..')
            df_basics  = pd.read_csv(data_uno,  delimiter ='\t')
            df_ratings = pd.read_csv(data_dos,  delimiter ='\t')
            df = df_ratings.merge(df_basics, how='inner', on='tconst')
            return df
        
        df = _get_merge( f"{CUR_DIR}/data_csv/{DATA_BASICS}.csv",  f"{CUR_DIR}/data_csv/{DATA_RATINGS}.csv")
        
        def _clean_df(dataframe):
            columns = ['startYear', 'genres', 'averageRating', 'numVotes', 'runtimeMinutes']
            print('Dimension inicial del Data Frame: {}'.format(dataframe.shape))
            for col in columns:
                dataframe = dataframe[(dataframe[col] != '\\N')]
            return dataframe
        
        df = _clean_df(df)
        
        def _transform_values(dataframe):
            columns = ['startYear', 'averageRating', 'numVotes', 'runtimeMinutes']
            for col in columns:
                if dataframe[col].dtype != float:
                    dataframe[col] = dataframe[col].astype(str).astype(int)
            return dataframe
        
        df = _transform_values(df)
            
        def _filter_agg_df(dataframe):
            years_list = list(dataframe['startYear'].unique())
            years_list.sort(reverse=True)
            dataframe = dataframe[dataframe.startYear.isin(years_list[:5]) & (dataframe.titleType == 'movie')]
            print('Dimension final del Data Frame antes de la agregacion: {}'.format(dataframe.shape))
            dataframe = dataframe.groupby(['startYear', 'genres'], 
                          as_index=False).agg({'averageRating': 'mean',
                                                'numVotes': 'sum',
                                                'runtimeMinutes' : 'mean'}).round(2).sort_values(by=['averageRating','numVotes'], ascending=False)
            return dataframe
        
        df = _filter_agg_df(df)
        folder_out =  f"{CUR_DIR}/home/{NAME_DATA_LOAD}.csv"
        df.to_csv(folder_out)
        return 'Load Dataframe in {}'.format(folder_out)
        
