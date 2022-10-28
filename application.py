from typer import Typer

from src.config import cli, containers, database

def create_app() -> Typer:

    '''
        Inicializacao de todos os pacotes e configuracoes da aplicacao
        cli: Estou usando a lib Typer para fazer o sistema de cli.
        containers: O sistema de injecao de dependencias esta sendo feito com a lib dependecy-injector
        database: Como ORM uso o SQLAchemy
    '''
    container = containers.init_app()
    app = Typer()
    app.container = container
    cli.init_app(app)
    return app

app = create_app()

if __name__ == '__main__':
    app()