import unittest

from elasticsearch_dsl.connections import connections

import logging

logging.basicConfig(level=logging.DEBUG)  # debug level logging
connections.create_connection(hosts=['localhost:9200'])

##########################
## IMPORTING TESTS HERE ##
##########################
from elastic_pony.test.test_creation import FirstTestCase



if __name__ == '__main__':
    print("Testing: {}".format(FirstTestCase.__name__))
    unittest.main()
