import unittest

import odm


class TestModel(odm.Model):
    number = odm.IntegerField(required=True)


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

