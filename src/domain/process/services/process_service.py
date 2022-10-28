import os
from typing import List
import pandas as pd
from src.domain.process.models.adult_model import AdultModel
from src.domain.process.models.enum.colum_names import ColumnNamesEnum
from src.domain.process.repositories.process_repository import ProcessRepository
from src.config import environment


class ProcessService:

    '''
    Principal Service da aplicacao, aqui sao definidas as logicas de negocios e delega ao repository as acoes necessarias.
    '''

    def __init__(self, repository: ProcessRepository):
        self.repository = repository
    
    def process_file(self, file_name:str, last_index:int) -> bool:
        
        '''
        Processo de ETL comeca por aqui, desde a entrada dos dados para o pandas, ate a chamada do repository para registro no banco de dados.
        '''
        
        column_names = ColumnNamesEnum.as_list()
        file_path = self._get_file_path(file_name)
        
        df = pd.read_csv(file_path, names=column_names, header=None, sep=", ", skiprows=last_index, engine='python')
        if df.empty:
            return False

        adult_models = self._transform_to_model(df)
        self._load_data(adult_models)
        
        load_range = int(environment.get_item("LOAD_RAGE"))
        new_index = load_range + last_index
        self._set_last_index(file_name, new_index)
        
        return True
    
    def get_last_index(self, file_name:str) -> int:
        
        '''
        Sistema para pegar o ultimo registro processado pelo sistema e usado para pular para os proximos.
        '''
        
        file_path = self._get_log_file_path(file_name)
        if os.path.exists(file_path):
            with open(file_path,"r") as file_log:
                last_index = file_log.readline()
            return int(last_index)
        return 0
    
    def _set_last_index(self, file_name:str, last_index:int) -> bool:

        '''
        Salvando o ultimo registro processado pelo sistema em um arquivo no diretorio src/logs/{arquivo-referente-ao-log}
        '''

        file_path = self._get_log_file_path(file_name)
        
        with open(file_path, "w+") as file_log:
            file_log.write(str(last_index))
        return True


    def _transform_to_model(self, df_data) -> List[AdultModel]:

        '''
        Transportando os dados de df Pandas para as models Pydantic.
        Nesse processo, conseguimos validar o tipo dos dados.
        '''

        models = []
        df_dict = df_data.to_dict("records")
        load_range = int(environment.get_item("LOAD_RAGE"))
        for index, data in enumerate(df_dict):
            index += 1
            data['class_field'] = data['class_field'].replace(".", "")
            df_model = AdultModel(**data)
            models.append(df_model)

            if index == load_range:
                break

        return models

    def _load_data(self, adult_models) -> bool:

        '''
        Chama o repository para salvar os dados no banco de dados
        '''
        
        return self.repository.save_bulk(adult_models)

    def _get_file_path(self, file_name):

        '''
        Monta o caminho para a pasta onde estao os dados
        '''
        
        return f"{environment.get_item('DATA_FOLDER')}/{file_name}"

    def _get_log_file_path(self, file_name):

        '''
        Monta o caminho para a pasta onde estao os logs
        '''

        return f"{environment.get_item('DATA_LOG_FOLDER')}/{file_name}"
        