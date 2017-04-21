# -*- coding: utf-8 -*-
from DictObject import DictObject
from elasticsearch_dsl import Keyword, Mapping
from elasticsearch_dsl.connections import connections
from pony import orm
from luckydonaldUtils.exceptions import assert_or_raise as assert_type_or_raise
from luckydonaldUtils.logger import logging

logger = logging.getLogger(__name__)


class INT_PLACEHOLDER: pass


TYPE_MAPPING = {
    'str': 'text',
    'bool': 'boolean',
    'int': INT_PLACEHOLDER,
    'float': 'float',  # TODO: does Pony seperate float types into double?
    'date': 'date',
    'datetime': 'date',
    'Json': 'object',
    '': '',
}
INT_SIZE_MAPPING = {
    8: 'byte',
    16: 'short',
    24: 'integer',
    32: 'integer',
    64: 'long',
}


def create_mapping(db, table_clazz, index_name, using='default'):
    """
    Creates the mapping for a given class (pony.orm.Entity).
    
    :param db: The Database
    :type  db: pony.orm.Database
    
    :param table_clazz: Either a class, or a string with the class name.
    :type  table_clazz: str | class
     
    :param index_name: The elastic search index where the schema should be created in.
    :type  index_name: str
    
    :param using: The elastic search connection (elasticsearch_dsl.connections.connections) identifier
    :type  using: str
    :return: 
    """
    assert_type_or_raise(db, orm.Database)
    if isinstance(table_clazz, str):
        table_clazz = db.entities[table_clazz]
    # end if
    assert_type_or_raise(table_clazz, orm.core.EntityMeta)
    m = Mapping(table_clazz.__name__)
    for attr in table_clazz._attrs_:
        create_field(m, attr)
    m.save(index_name, using=using)
# end def


def create_field(m, attr, **kwargs):
    """
    Creates a new field into the elastic definition.
    :param m: The mapping to add to. 
    :param attr: the json describing the attribute
    :param kwargs: Optional attributes for `m.field(..., **kwargs)`
    :return: None
    """
    assert_type_or_raise(m, Mapping)
    assert_type_or_raise(attr, orm.core.Attribute)
    if attr.__class__.__name__ == 'PrimaryKey':  # kind
        kwargs.setdefault("fields", {})
        kwargs["fields"].setdefault('raw', Keyword())
    # end if
    kwargs = DictObject.objectify(kwargs)
    ## Get the elastic search type ##
    es_type = pony_to_es_type(attr)
    m.field(attr.name, es_type, **kwargs)


def pony_to_es_type(attr, is_top=True):
    if attr.reverse:
        return pony_to_es_type(attr.py_type._pk_, is_top=False)
    #  end if
    es_type = TYPE_MAPPING[attr.py_type.__name__]  # type
    if es_type == INT_PLACEHOLDER:
        if hasattr(attr, "converters") and len(attr.converters) > 0 and hasattr(attr.converters[0], "size"):
            es_type = INT_SIZE_MAPPING[attr.converters[0].size]
        elif "kwargs" in attr and "size" in attr.kwargs:  # normally the one above should be working.
            logger.debug("Using kwargs to determine int size instead of converters.")
            es_type = INT_SIZE_MAPPING[attr.kwargs.size]
        else:
            # assuming normal INTEGER
            es_type = INT_SIZE_MAPPING[8]
        # end if
    elif es_type == 'Decimal':
        raise NotImplementedError("Currently doesn't support other types as floats.")
    # end if
    return es_type
# end def

def test():
    create_mapping(db, 'Sticker')