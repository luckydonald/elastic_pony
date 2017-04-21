# -*- coding: utf-8 -*-
from elasticsearch_dsl import Search
from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import bulk as es_bulk
from luckydonaldUtils.logger import logging
from luckydonaldUtils.exceptions import assert_or_raise as assert_type_or_raise
from elastic_pony.data import save_element, bulk_element, default__to_dict
from pony import orm
from pony.orm.core import Entity

__author__ = 'luckydonald'
logger = logging.getLogger(__name__)


class ElasticPony(object):
    def __init__(self, db, index_name, using="default", tables=None):
        """
        New instance.
        
        :param db: the pony orm Database object 
    
        :param index_name: The elastic search index where the schema should be created in.
        :type  index_name: str
    
        :param using: The elastic search connection (elasticsearch_dsl.connections.connections) identifier
        :type  using: str
     
        :param tables: List of either a class, or a string with the class name.
                       Or `None` (default) to use all tables in the given database (`db`).
        :type  tables: None  |  list of (str|class)
        """
        self.db = db
        self.index_name = index_name
        self.using = using
        assert_type_or_raise(tables, list, None)
        if tables is not None:
            self.tables = [self.validate_table_class(t) for t in tables]
        else:
            logger.debug("tables=None, importing tables from db.")
            self.tables = self.db.entities.values()
    # end def

    def validate_table_class(self, table_clazz):
        """
        If the given table is just a string, self.db is used to retrieve the actual class.
        
        :param table_clazz: String or Class of a PonyORM Entity.
        :type  table_clazz: str | class
        
        :return: An entity class.
        :rtype orm.core.EntityMeta
        """
        if isinstance(table_clazz, str):
            table_clazz = self.db.entities[table_clazz]
        # end if
        assert_type_or_raise(table_clazz, orm.core.EntityMeta)
        return table_clazz
    # end def

    def generate_index(self):
        from elastic_pony.schema import create_mapping
        for table_clazz in self.tables:
            logger.debug("Generating mapping for " + table_clazz.__name__)
            create_mapping(self.db, table_clazz, self.index_name, using='default')
        # end for
    # end def

    def save(self, obj, to_dict_method=default__to_dict):
        """
        Saves a object to the elastic index.
        
        :param obj: A instance of an Entity class (class must be registered in the `self.tables` list.
        :type  obj: orm.core.EntityMeta
        
        :param to_dict_method: a function witch converts a database entity instance to a json serializable dict. Default: `obj.to_dict()`
        
        :return: Elasticsearch result.
        """
        return save_element(obj, self.index_name, self.using, to_dict_method=to_dict_method)
    # end if

    def to_bulk(self, obj, to_dict_method=default__to_dict):
        """
        Saves a object to the elastic index.

        :param obj: A instance of an Entity class (class must be registered in the `self.tables` list.
        :type  obj: orm.core.EntityMeta

        :param to_dict_method: a function witch converts a database entity instance to a json serializable dict. Default: `obj.to_dict()`

        :return: Elasticsearch result.
        """
        return save_element(obj, self.index_name, self.using, to_dict_method=to_dict_method)

    # end if

    def bulk_all(self, to_dict_method=default__to_dict):
        """
        Like `.save_all()`, but uses the bulk api to save more efficiently.
        """
        es = connections.get_connection(alias=self.using)
        all_generator = [bulk_element(e, self.index_name, to_dict_method=to_dict_method) for e in self.yield_all()]
        return es_bulk(es, all_generator)
    # end if

    def save_all(self):
        for elem in self.yield_all():
            self.save(elem)
        # end for
    # end def

    def yield_all(self):
        """
        This will yield all elements from all database entity classes in `self.tables`.
        :return: 
        """
        with orm.db_session:
            for table_clazz in self.tables:
                yield from self.yield_table(table_clazz)
            # end for
        # end with
    # end def

    def bulk_table(self, table_clazz, to_dict_method=default__to_dict):
        """
        Like `.save_all()`, but uses the bulk api to save more efficiently.
        :return: 
        """
        es = connections.get_connection(alias=self.using)
        all_generator = [bulk_element(e, self.index_name, to_dict_method=to_dict_method) for e in self.yield_table(table_clazz)]
        return es_bulk(es, all_generator)
    # end if

    def save_table(self, table_clazz, to_dict_method=default__to_dict):
        for elem in self.yield_table(table_clazz):
            self.save(elem, to_dict_method=to_dict_method)
        # end for
    # end def

    def yield_table(self, table_clazz):
        """
        For a given database entity class (a orm.core.Entity sublass), this will yield all entries.
        
        :param table_clazz: 
        """
        assert issubclass(table_clazz, Entity)
        page_size = 1000
        page_num = 1
        while True:
            with orm.db_session:
                objects = table_clazz\
                    .select()\
                    .order_by(table_clazz._pk_)\
                    .page(page_num, page_size)
                yield from objects
                if len(objects) < page_size:
                    break
                # end if
            # end with
            page_num += 1
        # end while
    # end def

    def search(self, **kwargs):
        del kwargs['index']  # so it is not duplicated
        return Search(
            index=self.index_name,
            **kwargs
        )
    # end def
# end class
