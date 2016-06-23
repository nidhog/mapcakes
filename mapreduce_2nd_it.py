import settings
from multiprocessing import Process

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
        note: this method should be implemented
        """
        pass

    def reducer(self, key, values_list):
        """Outputs a single value together with the provided key.
        Note: this function is to be implemented.

        :param key:
        :param value_list:
        note: this method should be implemented
        """
        pass

    def run_mapper(self, index):
        """Runs the implemented mapper

        :param index: the index of the thread to run on
        """
        pass

    def run_reducer(self, index):
        """Runs the implemented reducer

        :param index: the index of the thread to run on
        """
        pass

    def run(self):
        """Executes the map and reduce operations

        """
        # initialize mappers list
        map_workers = []
        # initialize reducers list
        rdc_workers = []
        # run the map step
        for thread_id in range(self.n_mappers):
            p = Process(target=self.run_mapper, args=(thread_id,))
            p.start()
            map_workers.append(p)
        [t.join() for t in map_workers]
        # run the reduce step
        for thread_id in range(self.n_reducers):
            p = Process(target=self.run_reducer, args=(thread_id,))
            p.start()
            map_workers.append(p)
        [t.join() for t in rdc_workers]