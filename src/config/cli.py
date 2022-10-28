from typer import Typer

from src.infra.cli import process

def init_app(app: Typer):
    '''
    Encapsulamento dos comandos para criacao de sub niveis e melhor organizacao do codigo
    '''
    app.add_typer(process.app, name='process')
