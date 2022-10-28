from dotenv import dotenv_values
import os

class Env:

    '''
    Usando a lib dotenv para carregar os dados do .env e definir alguns metodos que sao usados na aplicacao
    '''

    def __init__(self):
        env_path = os.getcwd()
        self._config = {
            **dotenv_values(f'{env_path}/.env'),
            **os.environ,
        }

    @property
    def config(self):
        return self._config
    
    @config.setter
    def config(self, config):
        self._config = config
    
    def get_item(self, key: str, default = None):
        if key in self.config:
            return self.config[key]
        return None

environment = Env()