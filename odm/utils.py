from inspect import isclass

import sqlalchemy as sa


def get_columns(mixed):
    """
    Return a collection of all Column objects for given SQLAlchemy
    object.
    The type of the collection depends on the type of the object to return the
    columns from.
    ::
        get_columns(User)
        get_columns(User())
        get_columns(User.__table__)
        get_columns(User.__mapper__)
        get_columns(sa.orm.aliased(User))
        get_columns(sa.orm.alised(User.__table__))

    :param mixed:
        SA Table object, SA Mapper, SA declarative class, SA declarative class
        instance or an alias of any of these objects
    """
    if isinstance(mixed, sa.Table):
        return mixed.c
    if isinstance(mixed, sa.orm.util.AliasedClass):
        return sa.inspect(mixed).mapper.columns
    if isinstance(mixed, sa.sql.selectable.Alias):
        return mixed.c
    if isinstance(mixed, sa.orm.Mapper):
        return mixed.columns
    if not isclass(mixed):
        mixed = mixed.__class__
    return sa.inspect(mixed).columns
