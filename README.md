# xsorm
轻量级的MySQL ORM工具

## Quick Start
### Simple Usage

```
from xsorm import SessionMaker, VarcharField, IntField
from xsorm.model import declarative_base
from xsorm.operation import Quote

config = {
    'user': 'user',
    'password': 'passwd',
    'host': '127.0.0.1',
    'database': 'db_name',
    'raise_on_warnings': True,
    'autocommit': False
}

Session = SessionMaker(**config)
session = Session()

Base = declarative_base()

class User(Base):
    id = IntField(primary_key=True, auto_increment=True)
    nickname = VarcharField(100, nullable=True)
    age = IntField()

if __name__ == '__main__':
    user = session.query(User).filter(User.id >= 5).\
        filter(~User.nickname.like('user%') | Quote((User.id == 5) & (User.age < 10))).order_by(User.id).one()
    print(user)
```
