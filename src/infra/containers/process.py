from dependency_injector import containers, providers

from src.domain.process.services.process_service import ProcessService


class ProcessServiceContainer(containers.DeclarativeContainer):

    '''
    Sub container que recebe as dependencias do `ProcessSerivce`
    '''

    repository = providers.Dependency()
    process_service = providers.Factory(ProcessService, repository=repository)