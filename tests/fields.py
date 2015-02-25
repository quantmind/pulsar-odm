import unittest
from datetime import datetime

import pytz

import odm

from .data.models import User, Session


class TestModel(odm.Model):
    number = odm.IntegerField(required=True)


class NoStr:

    def __str__(self):
        raise Exception


class FieldTests(unittest.TestCase):

    def test_ChoiceField(self):
        field = odm.ChoiceField()
        self.assertEqual(field.attrs.get('name'), None)
        #
        self.assertEqual(field.options.all(), ())
        attrs = field.getattrs()
        self.assertEqual(attrs['options'], ())
        #
        field = odm.ChoiceField(options=('bla', 'foo'))
        self.assertEqual(field.options.all(), ('bla', 'foo'))
        attrs = field.getattrs()
        self.assertEqual(attrs['options'], ('bla', 'foo'))

    def test_ChoiceFieldOptions(self):
        opts = [{'value': 'a', 'repr': 'foo'},
                {'value': 'b', 'repr': 'hello'}]
        field = odm.ChoiceField(options=opts)
        self.assertEqual(field.options.all(), opts)
        self.assertEqual(field.options.get_initial(), 'a')

    def test_options(self):
        opts = ('uno', 'due', 'tre')
        field = odm.ChoiceField(options=opts)
        self.assertEqual(field.options.all(), opts)
        self.assertEqual(field.options.get_initial(), 'uno')

    def test_integer(self):
        model = TestModel()
        model['number'] = 6
        self.assertEqual(model.number, 6)
        model['number'] = 'h'
        self.assertRaises(odm.ValidationError, model.get, 'number')

    def test_char_field(self):
        user = User(username='pippo', bla='foo')
        user['username'] = 78
        self.assertEqual(user.username, '78')

        def _():
            user['username'] = NoStr()
            value = user.username

        self.assertRaises(odm.ValidationError, _)

    def test_datetime_field(self):
        session = Session()
        self.assertFalse(session)
        self.assertEqual(session.expiry, None)
        self.assertRaises(KeyError, lambda: session['expiry'])
        #
        # Get store data
        store = odm.create_store('dummy://')
        data, action = store.model_data(session)
        self.assertEqual(action, odm.Command.INSERT)
        self.assertEqual(len(data), 1)
        self.assertTrue('expiry' in data)
        expiry = data['expiry']
        self.assertTrue(expiry.tzinfo)

        dt = datetime.now()
        session = Session(expiry=dt)
        self.assertNotEqual(session.expiry, dt)
        self.assertEqual(session.expiry, pytz.utc.localize(dt))
        #
        session = Session(expiry=dt)
        data, action = store.model_data(session)
        self.assertEqual(action, odm.Command.INSERT)
        self.assertEqual(len(data), 1)
        self.assertTrue('expiry' in data)
        expiry = data['expiry']
        self.assertTrue(expiry.tzinfo)

    def test_choice_field(self):
        choice = odm.ChoiceField()
        self.assertFalse(choice.multiple)
        choice = odm.ChoiceField(multiple=True)
        self.assertTrue(choice.multiple)
        language = User._meta.dfields['language']
        self.assertFalse(language.multiple)
        self.assertEqual(language.html_name(), 'language')
