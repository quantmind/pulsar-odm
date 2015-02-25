from datetime import datetime, timedelta

import odm


def default_expiry(model):
    return datetime.now() + timedelta(days=7)


class User(odm.Model):
    # username = odm.CharField(unique=True)
    username = odm.CharField()
    password = odm.CharField(required=False, hidden=True)
    first_name = odm.CharField(required=False, index=True)
    last_name = odm.CharField(required=False, index=True)
    email = odm.CharField(required=False, unique=True)
    is_active = odm.BooleanField(default=True)
    can_login = odm.BooleanField(default=True)
    is_superuser = odm.BooleanField(default=False)
    joined = odm.DateTimeField(default=lambda m: datetime.now())
    language = odm.ChoiceField(options=('italian', 'french', 'english',
                                        'spanish'))


class Session(odm.Model):
    expiry = odm.DateTimeField(default=default_expiry)
    user = odm.ForeignKey(User)


class Blog(odm.Model):
    published = odm.DateField()
    title = odm.CharField()
    body = odm.CharField()
