"""JSONType definition."""
import json

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON, JSONB


class JSONType(sa.types.TypeDecorator):
    """
    JSONType offers way of saving JSON data structures to database. On
    PostgreSQL the underlying implementation of this data type is 'json' while
    on other databases its simply 'text'.

    ::


        from odm.types import JSONType


        class Product(Base):
            __tablename__ = 'product'
            id = sa.Column(sa.Integer, autoincrement=True)
            name = sa.Column(sa.Unicode(50))
            details = sa.Column(JSONType)


        product = Product()
        product.details = {
            'color': 'red',
            'type': 'car',
            'max-speed': '400 mph'
        }
        session.commit()
    """
    impl = sa.UnicodeText

    def __init__(self, binary=True, impl=sa.UnicodeText, *args, **kwargs):
        self.binary = binary
        self.impl = impl
        super().__init__(*args, **kwargs)

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            # Use the native JSONB or JSON type.
            if self.binary:
                return dialect.type_descriptor(JSONB)
            else:
                return dialect.type_descriptor(JSON)
        else:
            return dialect.type_descriptor(self.impl)

    def process_bind_param(self, value, dialect):
        if dialect.name == 'postgresql':
            return value
        elif value is not None:
            return json.dumps(value)
        else:
            return value

    def process_result_value(self, value, dialect):
        if dialect.name == 'postgresql':
            return value
        elif value is not None:
            return json.loads(value)
        else:
            return value
