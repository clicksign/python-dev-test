from dependency_injector import containers, providers
from src.config.database import DB
from src.domain.process.repositories.process_repository import ProcessRepository

from src.infra.containers import ProcessServiceContainer


class Container(containers.DeclarativeContainer):
    
    ''' 
        Aqui sao criados os containers para a inversao de dependencia na aplicacao 

        O db foi criado como Singleton para que em toda a aplicacao seja usada a mesma instancia.

        No caso do ProcessService, criei um Container separado para receber suas dependencias.
    '''
    
    config = providers.Configuration()
    db = providers.Singleton(DB)
    process_repository_container = providers.Factory(ProcessRepository, db=db)
    process_service_container = providers.Container(ProcessServiceContainer, repository=process_repository_container)

def init_app() -> Container:
    return Container()
