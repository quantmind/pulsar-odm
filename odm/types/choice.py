from enum import Enum

from sqlalchemy import types

from pulsar import ImproperlyConfigured


class ScalarCoercible(object):

    def _coerce(self, value):
        raise NotImplementedError

    def coercion_listener(self, target, value, oldvalue, initiator):
        return self._coerce(value)


class Choice(object):

    def __init__(self, code, value):
        self.code = code
        self.value = value

    def __eq__(self, other):
        if isinstance(other, Choice):
            return self.code == other.code
        return other == self.code

    def __ne__(self, other):
        return not (self == other)

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return 'Choice(code={code}, value={value})'.format(
            code=self.code,
            value=self.value
        )


class ChoiceType(types.TypeDecorator, ScalarCoercible):
    impl = types.Unicode(255)

    def __init__(self, choices, impl=None):
        self.choices = choices

        if isinstance(choices, type) and issubclass(choices, Enum):
            self.type_impl = EnumTypeImpl(enum_class=choices)
        else:
            self.type_impl = ChoiceTypeImpl(choices=choices)

        if impl:
            self.impl = impl

    @property
    def python_type(self):
        return self.impl.python_type

    def _coerce(self, value):
        return self.type_impl._coerce(value)

    def process_bind_param(self, value, dialect):
        return self.type_impl.process_bind_param(value, dialect)

    def process_result_value(self, value, dialect):
        return self.type_impl.process_result_value(value, dialect)


class ChoiceTypeImpl(object):
    """The implementation for the ``Choice`` usage."""

    def __init__(self, choices):
        if not choices:
            raise ImproperlyConfigured(
                'ChoiceType needs list of choices defined.'
            )
        self.choices_dict = dict(choices)

    def _coerce(self, value):
        if value is None:
            return value
        if isinstance(value, Choice):
            return value
        return Choice(value, self.choices_dict[value])

    def process_bind_param(self, value, dialect):
        if value and isinstance(value, Choice):
            return value.code
        return value

    def process_result_value(self, value, dialect):
        if value:
            return Choice(value, self.choices_dict[value])
        return value


class EnumTypeImpl(object):
    """The implementation for the ``Enum`` usage."""

    def __init__(self, enum_class):
        if Enum is None:
            raise ImproperlyConfigured(
                "'enum34' package is required to use 'EnumType' in Python "
                "< 3.4"
            )
        if not issubclass(enum_class, Enum):
            raise ImproperlyConfigured(
                "EnumType needs a class of enum defined."
            )

        self.enum_class = enum_class

    def _coerce(self, value):
        return self.enum_class(value) if value else None

    def process_bind_param(self, value, dialect):
        return self.enum_class(value).value if value else None

    def process_result_value(self, value, dialect):
        return self.enum_class(value) if value else None
