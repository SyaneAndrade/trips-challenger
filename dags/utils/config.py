# -*- coding: utf-8 -*-
import yaml

class Config():

    def __init__(self, config_path):
        self.path = config_path

    def __load(self):
        with open(self.path) as config_file:
            return yaml.load(config_file)
    
    def get_config(self):
        return self.__load()


if __name__ == '__main__':
    config = Config("C:\\Users\\Syane\\Documents\\Projects\\jobsity-challenge\\config\\dag_ingestions_csv.yaml")
    print(config.get_config())
