import settings

class MapReduce(object):
    """MapReduce class representing the mapreduce model

    note: the 'mapper' and 'reducer' methods must be
    implemented to use the mapreduce model.
    """
    def __init__(self, input_dir = settings.default_input_dir, output_dir = settings.default_output_dir,
                 n_mappers = settings.default_n_mappers, n_reducers = settings.default_n_reducers,
                 clean = True):
        """

        :param input_dir: directory of the input files,
        taken from the default settings if not provided
        :param output_dir: directory of the output files,
        taken from the default settings if not provided
        :param n_mappers: number of mapper threads to use,
        taken from the default settings if not provided
        :param n_reducers: number of reducer threads to use,
        taken from the default settings if not provided
        :param clean: optional, if True temporary files are
        deleted, True by default.
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.clean = clean

    def mapper(self, key, value):
        """outputs a list of key-value pairs, where the key is
        potentially new and the values are of a potentially different type.
        Note: this function is to be implemented.

        :param key:
        :param value:

        """
        pass

    def reducer(self, key, values_list):
        """Outputs a single value together with the provided key.
        Note: this function is to be implemented.

        :param key:
        :param value_list:

        """
        pass