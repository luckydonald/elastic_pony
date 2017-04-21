from elasticsearch import Elasticsearch
from luckydonaldUtils.exceptions import assert_or_raise as assert_type_or_raise
from elasticsearch_dsl.connections import connections
from pony import orm


def default__to_dict(obj):
    return obj.to_dict()
# end def


def save_element(obj, index_name, using='default', to_dict_method=default__to_dict):
    """
    This saves an element to elasticsearch.
    
    :param obj: the database entity instance (i.e. a row) 
    :param index_name: The elastic search index where the schema should be created in.
    :param using: The elastic search connection (elasticsearch_dsl.connections.connections) identifier
    :param to_dict_method: a function witch converts a database entity instance to a json serializable dict.
                           Default: `obj.to_dict()`
    :return: The result of the index operation.
    """
    assert_type_or_raise(obj, orm.core.Entity)
    es = connections.get_connection(alias=using)

    with orm.db_session:
        res = es.index(index=index_name, doc_type=obj.__class__.__name__, id=obj._pk_, body=to_dict_method(obj))
    # end with
    return res
# end def


def bulk_element(obj, index_name, to_dict_method=default__to_dict):
    """
    This generates a (json) dict for bulk insert of an Pony ORM database entity object.  

    :param obj: the database entity instance (i.e. a row) 
    :param index_name: The elastic search index where the schema should be created in.
    :param using: The elastic search connection (elasticsearch_dsl.connections.connections) identifier
    :param to_dict_method: a function witch converts a database entity instance to a json serializable dict.
                           Default: `obj.to_dict()`
    :return: The result of the index operation.
    """
    data = to_dict_method(obj)
    data['_type'] = obj.__class__.__name__
    data['_index'] = index_name
    data['_id'] = obj._pk_
    return data
# end def
