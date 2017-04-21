import unittest

from elasticsearch_dsl.connections import connections
from pony import orm
from .. import ElasticPony
from datetime import datetime

no_elasticsearch = False
try:
    connections.get_connection().info()  # Check elasticsearch connectivity.
except Exception as e:
    no_elasticsearch = RuntimeError("Could not connect to Elasticsearch.\n"
          "Check that you set a connection with elasticsearch_dsl.\n"
          "Example:\n"
          ">>> from elasticsearch_dsl.connections import connections\n"
          ">>> connections.create_connection(hosts=['localhost:9200'])\n"
          "This will set the 'default' connection.\n"
          "By the way, the reported error was {e!r}".format(e=str(e)))
# end try
if no_elasticsearch:
    raise no_elasticsearch
# end


class FirstTestCase(unittest.TestCase):
    def setUp(self):
        from .database import create_testclass
        from .data import create_testclass_data



        self.test_index = "test_" + ((datetime.now().isoformat()).replace(":","_").replace(".","_").replace("-","_")).lower()

        db = orm.Database()
        Testclass = create_testclass(db)

        db.bind("sqlite", ":memory:", create_db=True)
        db.generate_mapping(create_tables=True)
        with orm.db_session:
            self.a, self.b = create_testclass_data(Testclass)
        # end if
        assert isinstance(self.a, orm.core.Entity)

        self.ep = ElasticPony(db, self.test_index)
    # end def

    def test__generate_index__works(self):
        self.ep.generate_index()
        print("ELASTIC SEARCH INDEX test__generate_index__works " + self.test_index)
    # end def

    def test__save__works(self):
        print("ELASTIC SEARCH INDEX test__save__works " + self.test_index)
        self.ep.save(self.a)
    # end def

    def test__generate_index__and__save__works(self):
        print("ELASTIC SEARCH INDEX test__generate_index__and__save__works " + self.test_index)
        self.ep.generate_index()
        self.ep.save(self.a)
    # end def

    def test__save_all__works(self):
        print("ELASTIC SEARCH INDEX test__save_all__works " + self.test_index)
        self.ep.save_all()
    # end def


if __name__ == '__main__':
    unittest.main()