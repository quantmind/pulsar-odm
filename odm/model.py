import logging

from sqlalchemy.ext.declarative import declarative_base, declared_attr


logger = logging.getLogger('lux.odm')


class BaseModel(object):

    @declared_attr
    def __tablename__(self):
        return self.__name__.lower()


Model = declarative_base(cls=BaseModel)
