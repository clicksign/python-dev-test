from typing import List
from src.domain.process.models.adult_model import AdultModel
from src.infra.entity.adult import Adult
from src.shared.repository.base_repository import BaseRepository


class ProcessRepository(BaseRepository):

    '''
    Comunicando com o banco de dados e fazendo transportando a camada de dados da aplicacao
    '''
    
    def save_bulk(self, data: List[AdultModel]) -> bool:

        '''
        Transcrevendo a model para a entity e salvando em bulks no banco de dados
        '''

        adult_entities = []
        for adult_model in data:
            adult_entity = Adult()
            for adult_model_field in list(adult_model.dict().keys()):
                setattr(adult_entity, adult_model_field, getattr(adult_model, adult_model_field))
            adult_entities.append(adult_entity)

        self.entity = adult_entities
        self.save(bulk=True)
        return True