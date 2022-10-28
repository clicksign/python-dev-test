import typer
import pandas as pd
from src.config.containers import Container
from src.config import environment
from src.domain.process.services.process_service import ProcessService


app = typer.Typer()

container = Container()

@app.command()
def main():

    '''
    Comando principal que atua como route ou view do sistema, 
    aqui esta acontecendo a entrada de dados e a chamada da classe service da aplicacao.
    '''

    service : ProcessService  = container.process_service_container.process_service() # Sistema de injecao de dependencias para pegar uma instancia de ProcessService
    file_names = environment.get_item("FILE_NAMES").split(",")
    for file_name in file_names:
        last_index = service.get_last_index(file_name)
        if "test" in file_name:
            last_index += 1

        process_response = service.process_file(file_name, last_index)
        if process_response:
            break